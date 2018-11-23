package org.linkeddatafragments.datasource.cache;

import java.util.HashMap;
import java.util.Map;

/**
 * Cache is a LRU cache built with a HashMap with pointers to a Doubly Linked List, allowing
 * fast insertion and removal.
 *
 * @param <K> The key type.
 * @param <V> The value type.
 */
public class Cache<K, V> {
    private Map<K, Node<K, V>> fastAccess = new HashMap<>();
    private DoublyLinkedList<K, V> list = new DoublyLinkedList<>();

    private static Map<String, Cache> instances = new HashMap<>();

    private int maxSize;

    public int misses;
    public int oks;

    public Cache(int maxSize) {
        this.maxSize = maxSize;
    }

    public V find(K key) {
        long currentTime = System.currentTimeMillis();

        if (fastAccess.containsKey(key)) {
            Node<K, V> node = fastAccess.get(key);

            // remove node if ttl did hit zero.
            if (!node.isAlive(currentTime)) {
                list.remove(node);
                return null;
            }

            // move node to the first position.
            list.remove(node);
            list.prepend(node);

            oks++;

            return node.value;
        }

        misses++;

        return null;
    }

    public synchronized void insert(K key, V value, int ttl) {
        if (fastAccess.containsKey(key)) {
            // if the key is already known, interpret it as an access.
            find(key);
            return;
        }

        Node<K, V> node = new Node<>(key, value, ttl);
        node.timeCreated = System.currentTimeMillis();

        clearSpaceIfNeeded();
        list.prepend(node);
        fastAccess.put(node.key, node);
    }

    private void clearSpaceIfNeeded() {
        if (list.size() < maxSize) {
            return;
        }

        Node<K, V> node = list.getTail();
        list.remove(node);
        fastAccess.remove(node.key);
    }

    public static <K, V> Cache<K, V> getInstance(Class<K> cls1, Class<V> cls2, int maxSize) {
        String code = cls1.getName() + ":" + cls2.getName();

        if (!instances.containsKey(code)) {
            instances.put(code, new Cache<K, V>(maxSize));
        }

        return (Cache<K, V>) instances.get(code);
    }

    public void launchVulture() {
        // TODO.
    }

    public void debugList() {
        list.debugList();
    }
}
