package io.lygoing.redis;

import io.lygoing.redis.utils.ConfigUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Response;

import java.nio.charset.StandardCharsets;

/**
 * @author Mr.Luo
 */
public class RedisVerticle extends AbstractVerticle {

    Redis client;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        JsonObject config = context.config();
        RedisOptions options = getRedisOptions(config);
        EventBus eb = vertx.eventBus();
        this.client = Redis.createClient(vertx, options).connect(onConnect -> {
            if (onConnect.succeeded()) {
                onConnect.result().close();
                eb.<String>consumer(Events.GET_ROUTING_CACHE, message -> getRouting(message.body()).onComplete(ar -> message.reply(ar.result())));
                eb.<CacheEntry<String, String>>consumer(Events.PUT_ROUTING_CACHE, message -> putRouting(message.body()).onComplete(ar -> message.reply(ar.succeeded())));
                startPromise.complete();
            } else {
                Throwable throwable = onConnect.cause();
                startPromise.fail(throwable);
            }
        });
        super.start();
    }

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        if (this.client != null) {
            this.client.close();
        }
        stopPromise.complete();
    }

    public RedisOptions getRedisOptions(JsonObject config) {
        JsonObject options = ConfigUtils.getConfigValue(config, "redis.cache-server");
        return options == null ? new RedisOptions() : new RedisOptions(options);
    }

    public Future<String> getRouting(String key) {
        Promise<String> promise = Promise.promise();
        RedisAPI redis = RedisAPI.api(client);
        redis.get(key, ar -> {
            if (ar.succeeded()) {
                Response response = ar.result();
                if (response == null) {
                    promise.complete(null);
                } else {
                    promise.complete(response.toString(StandardCharsets.UTF_8));
                }
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    public Future<Void> putRouting(CacheEntry<String, String> entry) {
        Promise<Void> promise = Promise.promise();
        RedisAPI redis = RedisAPI.api(client);
        redis.setex(entry.getKey(), String.valueOf(entry.getTtl()), entry.getValue(), ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
}
