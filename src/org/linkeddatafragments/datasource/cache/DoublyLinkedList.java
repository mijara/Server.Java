package org.linkeddatafragments.datasource.cache;

public class DoublyLinkedList<K, V> {
    private int listSize;

    private Node<K, V> head;
    private Node<K, V> tail;

    public synchronized void prepend(Node<K, V> node) {
        node.next = head;

        if (head != null) {
            head.previous = node;
        }

        head = node;

        if (tail == null) {
            tail = node;
        }

        listSize++;
    }

    public synchronized void remove(Node<K, V> node) {
        if (node.previous != null) {
            node.previous.next = node.next;
        } else {
            head = node.next;
        }

        if (node.next != null) {
            node.next.previous = node.previous;
        } else {
            tail = node.previous;
        }

        node.previous = null;
        node.next = null;

        listSize--;
    }

    public int size() {
        return listSize;
    }

    public Node<K, V> getHead() {
        return head;
    }

    public Node<K, V> getTail() {
        return tail;
    }
}
