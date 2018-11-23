package org.linkeddatafragments.datasource.cache;

public class Node<K, V> {
    public K key;
    public V value;

    public long ttl;
    public long timeCreated;

    public Node<K, V> next;
    public Node<K, V> previous;

    public Node(K key, V value, int ttl) {
        this.key = key;
        this.value = value;
        this.ttl = ttl;
    }

    public boolean isAlive(long currentTime) {
        return ttl < 0 || currentTime - timeCreated <= ttl;
    }
}
