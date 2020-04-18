package io.lygoing.redis;

import io.vertx.core.shareddata.Shareable;

/**
 * @author Mr.Luo
 */
public class CacheEntry<K, V> implements Shareable {

    private K key;
    private V value;
    private int ttl;

    public CacheEntry() {
    }

    public CacheEntry(K key, V value, int ttl) {
        this.key = key;
        this.value = value;
        this.ttl = ttl;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }
}
