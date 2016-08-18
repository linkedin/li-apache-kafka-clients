/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.utils;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A util class that helps maintain the insertion order of keys to a Java map. This class allows O(1) removal of keys
 * based on their insertion order. This class is not thread safe.
 * <p>
 * There are some classes in the Java library that supports similar function but not exactly what we need.
 * A TreeMap sorts the entries by key and can get the smallest key in O(1), but since our key is message ID so we will
 * need an additional data structure to maintain the order.
 * A LinkedMap maintains the insertion order but can only remove the eldest key when inserting a new entry into the
 * map.
 */
public class QueuedMap<K, V> {
  private ConcurrentHashMap<K, MapValue> _map;
  private DoublyLinkedList _queue;

  public QueuedMap() {
    _map = new ConcurrentHashMap<>();
    _queue = new DoublyLinkedList();
  }

  /**
   * Put a key-value pair into the map.
   *
   * @param k key
   * @param v value
   */
  public void put(K k, V v) {
    remove(k);
    MapValue mapValue = new MapValue(v);
    mapValue.node = _queue.addKey(k);
    _map.put(k, mapValue);
  }

  /**
   * Get value based on key.
   *
   * @param k key
   * @return value associated with the key if it exists, otherwise returns null.
   */
  public V get(K k) {
    MapValue mapValue = _map.get(k);
    return mapValue == null ? null : mapValue.value;
  }

  /**
   * Remove a key-value pair from the map.
   *
   * @param k key
   * @return value associated with the key if it exists, otherwise returns null.
   */
  public V remove(K k) {
    MapValue mapValue = _map.remove(k);
    if (mapValue != null) {
      _queue.remove(mapValue.node);
    }
    return mapValue == null ? null : mapValue.value;
  }

  /**
   * Get the eldest key inserted into the map.
   *
   * @return the eldest key in the map.
   */
  public K getEldestKey() {
    ListNode node = _queue.peek();
    return node == null ? null : node.key;

  }

  public int size() {
    return _map.size();
  }

  public void clear() {
    _map.clear();
    _queue = new DoublyLinkedList();
  }

  // Helper classes
  private class MapValue {
    V value;
    ListNode node;

    public MapValue(V v) {
      value = v;
    }
  }

  private class ListNode {
    ListNode prev;
    ListNode next;
    K key;

    public ListNode(K k) {
      prev = null;
      next = null;
      key = k;
    }
  }

  private class DoublyLinkedList {
    private ListNode _head;
    private ListNode _tail;
    private int _size;

    public DoublyLinkedList() {
      _head = null;
      _tail = null;
      _size = 0;
    }

    public synchronized void add(ListNode node) {
      if (_head == null) {
        _head = node;
        _tail = node;
        node.prev = null;
        node.next = null;
      } else {
        _tail.next = node;
        node.prev = _tail;
        node.next = null;
        _tail = node;
      }
    }

    public synchronized ListNode addKey(K key) {
      ListNode node = new ListNode(key);
      add(node);
      return node;
    }

    public synchronized ListNode peek() {
      return _head;
    }

    public synchronized void remove(ListNode node) {
      if (node != _head) {
        node.prev.next = node.next;
      } else {
        _head = node.next;
      }
      if (node != _tail) {
        node.next.prev = node.prev;
      } else {
        _tail = node.prev;
      }

      node.next = null;
      node.prev = null;
      _size -= 1;
    }

    public synchronized int size() {
      return _size;
    }
  }
}
