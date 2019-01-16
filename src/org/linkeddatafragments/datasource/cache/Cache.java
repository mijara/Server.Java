package org.linkeddatafragments.datasource.cache;

public interface Cache<K, V> {
    V find(K key);
    void insert(K key, V value);
}
