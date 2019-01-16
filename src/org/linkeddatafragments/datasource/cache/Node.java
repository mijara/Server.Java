package org.linkeddatafragments.datasource.cache;

public class Node<K, V> {
    public K key;
    public V value;

    public Node<K, V> next;
    public Node<K, V> previous;

    public Node(K key, V value) {
        this.key = key;
        this.value = value;
    }
}
