package com.sinobridge.demo;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import reactor.core.publisher.Mono;


import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;


import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class AsyncMysqlExample {

    public static class AsyncMysqlLookupFunction extends RichAsyncFunction<String, String> {
        private static final long serialVersionUID = 1L;

        private transient ConnectionPool connectionPool;
        private final int maxRetries; //最大重试次数
        private final Duration retryInterval; //重试间隔

        public AsyncMysqlLookupFunction(int maxRetries, Duration retryInterval) {
            this.maxRetries = maxRetries;
            this.retryInterval = retryInterval;
        }

        @Override
        public void open(Configuration parameters) {
            ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                    .option(DRIVER, "mysql")
                    .option(HOST, "192.168.250.20")
                    .option(PORT, 3306)
                    .option(USER, "root")
                    .option(PASSWORD, "123456")
                    .option(DATABASE, "flink")
                    .build());

            ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                    .maxIdleTime(Duration.ofMinutes(30)) //连接池中连接的最大空闲时间。当连接在连接池中空闲超过这个时间后，连接将被关闭并从连接池中移除
                    .initialSize(5)
                    .maxSize(20)
                    .build();

            connectionPool = new ConnectionPool(configuration);
        }

        @Override
        public void asyncInvoke(String key, ResultFuture<String> resultFuture) {
            queryMysqlWithRetries(key, resultFuture);
        }


        /*private void queryMysql(String key, ResultFuture<String> resultFuture) {
            Mono.from(connectionPool.create())
                    .flatMapMany(connection -> connection
                            .createStatement("SELECT value FROM t01 WHERE id = ?")
                            .bind(0, key)
                            .execute())
                    .flatMap(result -> result.map((row, rowMetadata) -> row.get("value", String.class)))
                    .collectList()
                    .subscribe(
                            result -> {
                                if (result.isEmpty()) {
                                    resultFuture.complete(Collections.singleton("No result found for key: " + key));
                                } else {
                                    resultFuture.complete(result);
                                }
                            },
                            throwable -> resultFuture.completeExceptionally(throwable)
                    );
        }*/

        private Mono<List<String>> executeQueryWithRetry(String key, int currentRetry) {
            return Mono.from(connectionPool.create())
                    .flatMapMany(connection -> connection
                            .createStatement("SELECT value FROM t01 WHERE id = ?") //测试sql: create table t01(id varchar(20), value varchar(50));
                            .bind(0, key)
                            .execute())
                    .flatMap(result -> result.map((row, rowMetadata) -> row.get("value", String.class)))
                    .collectList()
                    .flatMap(result -> {
                        if (result.isEmpty()) {
                            if (currentRetry < maxRetries) {
                                return Mono.delay(retryInterval)
                                        .then(executeQueryWithRetry(key, currentRetry + 1));
                            } else {
                                return Mono.just(Collections.singletonList("No result found for key: " + key));
                            }
                        } else {
                            return Mono.just(result);
                        }
                    });
        }

        private void queryMysqlWithRetries(String key, ResultFuture<String> resultFuture) {
            executeQueryWithRetry(key, 0)
                    .subscribe(
                            result -> resultFuture.complete(result),
                            throwable -> resultFuture.completeExceptionally(throwable)
                    );
        }



        @Override
        public void timeout(String input, ResultFuture<String> resultFuture) {
            resultFuture.complete(Collections.singleton("Query timeout for key: " + input));
        }

        @Override
        public void close() {
            if (connectionPool != null) {
                connectionPool.dispose();
            }
        }


    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputStream = env.addSource(new SourceFunction<String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void run(SourceContext<String> ctx) {
                // Generate sample keys
                for (int i = 0; i < 1000; i++) {
                    ctx.collect("key_" + i);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public void cancel() {
            }
        });

        DataStream<String> resultStream = AsyncDataStream
                .unorderedWait(
                        inputStream,
                        /*new AsyncMysqlLookupFunction(10, Duration.ofMinutes(3)),
                        1800000, // 30-minute timeout*/
                        new AsyncMysqlLookupFunction(20, Duration.ofSeconds(3)),
                        1800000, // 30-minute timeout
                        TimeUnit.MILLISECONDS,
                        100 // max concurrent requests
                );

        resultStream.print();

        env.execute("Async MySQL Example");
    }
}

