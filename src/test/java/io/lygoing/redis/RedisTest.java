package io.lygoing.redis;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * @author Luo Yong
 */
@ExtendWith(VertxExtension.class)
public class RedisTest {

    public static final String TEST_KEY = "test";

    Vertx vertx_;
    JsonObject config_;

    @Test
    public void get(VertxTestContext testContext) throws Throwable {
        EventBus eventBus = vertx_.eventBus();
        eventBus.request(Events.GET_ROUTING_CACHE, TEST_KEY, ar -> {
            if (ar.succeeded()) {
                System.out.println(ar.result().body());
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });

        testContext.awaitCompletion(5, TimeUnit.SECONDS);
        if (testContext.failed()) {
            throw testContext.causeOfFailure();
        }
    }

    @Test
    public void put(VertxTestContext testContext) throws Throwable {
        EventBus eventBus = vertx_.eventBus();
        eventBus.registerDefaultCodec(CacheEntry.class, new CacheEntryCodec());
        eventBus.request(Events.PUT_ROUTING_CACHE, new CacheEntry<>(TEST_KEY, "test2222", 3600), ar -> {
            if (ar.succeeded()) {
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });

        testContext.awaitCompletion(5, TimeUnit.SECONDS);
        if (testContext.failed()) {
            throw testContext.causeOfFailure();
        }
    }

    @Test
    public void doGetBenchmark() throws InterruptedException {
        int concurrent = 5000;
        int count = concurrent * 10;
        CountDownLatch latch = new CountDownLatch(count);
        AtomicInteger errors = new AtomicInteger(0);
        ArrayBlockingQueue<Future<Void>> queue = new ArrayBlockingQueue<>(concurrent);
        long end, start = System.currentTimeMillis();
        while (latch.getCount() > 0) {
            Future<Void> future = doGet();
            queue.put(future);
            future.onComplete(ar -> {
                if (ar.failed()) {
                    errors.addAndGet(1);
                }
                latch.countDown();
                queue.remove(future);
            });
        }
        latch.await();
        end = System.currentTimeMillis();
        System.out.println("Throughput: " + count / ((end - start) / 1000.0) + "/s");
        System.out.println("Errors: " + errors.get());
    }

    public Future<Void> doGet() {
        EventBus eventBus = vertx_.eventBus();
        Promise<Void> promise = Promise.promise();
        eventBus.<String>request(Events.GET_ROUTING_CACHE, TEST_KEY, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    @BeforeEach
    public void doLauncher() throws Throwable {
        Promise<Void> promise = Promise.promise();
        CountDownLatch latch = new CountDownLatch(1);
        new SafeLauncher() {

            @Override
            public List<Future<String>> getDeployVerticleFuture(Vertx vertx, JsonObject config) {
                vertx_ = vertx;
                config_ = config;
                int processors = Runtime.getRuntime().availableProcessors();
                return List.of(deployVerticle(vertx, RedisVerticle.class, new DeploymentOptions().setInstances(processors * 2), config));
            }

            @Override
            public BiConsumer<AsyncResult<CompositeFuture>, Vertx> afterDeployCallback(Vertx vertx, JsonObject config) {
                return (deploy, vx) -> {
                    if (deploy.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(deploy.cause());
                    }
                    latch.countDown();
                };
            }
        }.dispatch(null);

        latch.await();
        if (promise.future().cause() != null) {
            throw promise.future().cause();
        }
    }

}
