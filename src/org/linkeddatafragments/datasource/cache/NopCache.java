package org.linkeddatafragments.datasource.cache;

public class NopCache<K, V> implements Cache<K, V> {
    @Override
    public V find(K key) {
        return null;
    }

    @Override
    public void insert(K key, V value) {
    }
}
