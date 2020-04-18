package io.lygoing.redis;

import io.lygoing.redis.utils.ConfigUtils;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author Mr.Luo
 */
public abstract class SafeLauncher {

    private final Logger log = LoggerFactory.getLogger(this.getClass().getName());

    public static final String DEFAULT_VERTX_OPTIONS_KEY = "vertx.main";

    public final Future<String> deployVerticle(Vertx vertx, Verticle verticle, JsonObject config) {
        return deployVerticle(vertx, verticle, new DeploymentOptions(), config);
    }

    public final Future<String> deployVerticle(Vertx vertx, Verticle verticle, DeploymentOptions options, JsonObject config) {
        beforeDeployingVerticle(options, config);
        return deployVerticle((promise) -> vertx.deployVerticle(verticle, options, deployHandle(promise, verticle.getClass().getName())));
    }

    public final Future<String> deployVerticle(Vertx vertx, Class<? extends Verticle> clazz, DeploymentOptions options, JsonObject config) {
        beforeDeployingVerticle(options, config);
        return deployVerticle((promise) -> vertx.deployVerticle(clazz, options, deployHandle(promise, clazz.getName())));
    }

    public final Future<String> deployVerticle(Vertx vertx, String name, JsonObject config) {
        return deployVerticle(vertx, name, new DeploymentOptions(), config);
    }

    public final Future<String> deployVerticle(Vertx vertx, String name, DeploymentOptions options, JsonObject config) {
        beforeDeployingVerticle(options, config);
        return deployVerticle((promise) -> vertx.deployVerticle(name, options, deployHandle(promise, name)));
    }

    private Future<String> deployVerticle(Consumer<Promise<String>> consumer) {
        Promise<String> promise = Promise.promise();
        consumer.accept(promise);
        return promise.future();
    }

    Handler<AsyncResult<String>> deployHandle(Promise<String> promise, String name) {
        return ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                Throwable throwable = ar.cause();
                log.error("[{}] Verticle deploy failed.", name, throwable);
                promise.fail(ar.cause());
            }
        };
    }

    public void beforeDeployingVerticle(DeploymentOptions deploymentOptions, JsonObject config) {
        deploymentOptions.setConfig(config);
    }

    public JsonObject getConfiguration(String[] args) throws Exception {
        return ConfigUtils.readConfigInClasspath("application-test.yml", "application.yml");
    }

    public VertxOptions getVertxOptions(JsonObject config) {
        JsonObject json = ConfigUtils.getConfigValue(config, DEFAULT_VERTX_OPTIONS_KEY);
        return json == null ? new VertxOptions() : new VertxOptions(json);
    }

    public void beforeStartingVertx(VertxOptions options) {

    }

    public void afterStartingVertx(Vertx vertx, JsonObject config) {
        List<Future<String>> list = getDeployVerticleFuture(vertx, config);
        if (!(list == null || list.isEmpty())) {
            CompositeFuture.all(new ArrayList<>(list)).onComplete(ar ->
                    afterDeployCallback(vertx, config).accept(ar, vertx));
        }
    }

    public abstract List<Future<String>> getDeployVerticleFuture(Vertx vertx, JsonObject config);

    public BiConsumer<AsyncResult<CompositeFuture>, Vertx> afterDeployCallback(Vertx vertx, JsonObject config) {
        return (ar, vertx_) -> {
            if (ar.failed()) {
                vertx_.close();
            }
        };
    }

    public void beforeStoppingVertx(Vertx vertx, JsonObject config) {

    }

    public void afterStoppingVertx(JsonObject config) {

    }

    public void dispatch(String[] args) throws Exception {
        //step 1:
        JsonObject config = getConfiguration(args);
        //step 2:
        VertxOptions options = getVertxOptions(config);
        beforeStartingVertx(options);
        //step 3:
        Vertx vertx = Vertx.vertx(options);
        //step 4:
        afterStartingVertx(vertx, config);
        //step 5:
        addShutdownHook(vertx, config);
    }

    protected void addShutdownHook(Vertx vertx, JsonObject config) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            beforeStoppingVertx(vertx, config);
            CountDownLatch latch = new CountDownLatch(1);
            System.out.println("Vert.x stopping...");
            vertx.close(ar -> {
                if (ar.failed()) {
                    System.err.println("Failure in stopping Vert.x.");
                    ar.cause().printStackTrace();
                }
                latch.countDown();
            });
            try {
                if (!latch.await(2, TimeUnit.MINUTES)) {
                    System.err.println("Timed out waiting to undeploy all.");
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            } finally {
                afterStoppingVertx(config);
                System.out.println("Vert.x stopped.");
            }
        }));
    }

}
