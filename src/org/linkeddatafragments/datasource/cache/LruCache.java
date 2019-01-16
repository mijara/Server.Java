package org.linkeddatafragments.datasource.cache;

import java.util.HashMap;
import java.util.Map;

/**
 * LruCache is a LRU cache built with a HashMap with pointers to a Doubly Linked List, allowing
 * fast insertion and removal.
 *
 * @param <K> The key type.
 * @param <V> The value type.
 */
public class LruCache<K, V> implements Cache<K, V> {
    private Map<K, Node<K, V>> fastAccess = new HashMap<>();
    private DoublyLinkedList<K, V> list = new DoublyLinkedList<>();

    private int maxSize;

    public LruCache(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public V find(K key) {
        if (fastAccess.containsKey(key)) {
            return reinsertNode(key).value;
        }

        return null;
    }

    @Override
    public synchronized void insert(K key, V value) {
        // check if we already know this key.
        if (fastAccess.containsKey(key)) {
            // if the key is known, then reinsert it so that it jumps back to the top
            // of the list.
            reinsertNode(key);
            return;
        }

        // otherwise, insert a node.
        insertNode(key, value);
    }

    private synchronized Node<K, V> insertNode(K key, V value) {
        // create the node and insert in the list. Registering the key on the fast access table.
        Node<K, V> node = new Node<>(key, value);
        clearSpaceIfNeeded();
        list.prepend(node);
        fastAccess.put(node.key, node);
        return node;
    }

    private synchronized Node<K, V> reinsertNode(K key) {
        Node<K, V> node = fastAccess.get(key);

        // move node to the first position.
        list.remove(node);
        list.prepend(node);

        return node;
    }

    private void clearSpaceIfNeeded() {
        if (list.size() < maxSize) {
            return;
        }

        Node<K, V> node = list.getTail();
        list.remove(node);
        fastAccess.remove(node.key);
    }

    public static void main(String[] args) {
        Cache<String, String> cache = new LruCache<>(2);
        cache.insert("Key #1", "Value #1");
        cache.insert("Key #2", "Value #2");

        System.out.println(cache.find("Key #1"));
        System.out.println(cache.find("Key #2"));

        cache.insert("Key #3", "Value #3");

        System.out.println(cache.find("Key #1"));
        System.out.println(cache.find("Key #2"));
        System.out.println(cache.find("Key #3"));
    }
}
