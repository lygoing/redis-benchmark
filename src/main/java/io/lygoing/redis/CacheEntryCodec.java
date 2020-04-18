package io.lygoing.redis;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

/**
 * @author Mr.Luo
 */
public class CacheEntryCodec implements MessageCodec<CacheEntry, CacheEntry> {

    @Override
    public void encodeToWire(Buffer buffer, CacheEntry cacheEntry) {

    }

    @Override
    public CacheEntry decodeFromWire(int pos, Buffer buffer) {
        return null;
    }

    @Override
    public CacheEntry transform(CacheEntry cacheEntry) {
        return cacheEntry;
    }

    @Override
    public String name() {
        return this.getClass().getName();
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }
}
