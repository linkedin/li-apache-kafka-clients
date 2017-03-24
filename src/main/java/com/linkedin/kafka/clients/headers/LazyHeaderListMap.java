/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.headers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;


/**
 *  This is a specialized map backed by a list instead of a more asymptotically efficient data structure as our
 *  benchmarks show that for small numbers of items this will be more efficient than using HashMap.
 *
 *  This map does not check for duplicates in the incoming ByteBuffer.
 *
 */
public class LazyHeaderListMap implements Map<String, byte[]> {

  private static final class Entry implements Map.Entry<String, byte[]> {
    private final String key;
    private byte[] value;

    public Entry(String key, byte[] value) {
      HeaderUtils.validateHeaderKey(key);

      if (value == null) {
        throw new NullPointerException("value must not be null");
      }

      this.key = key;
      this.value = value;
    }

    @Override
    public String getKey() {
      return key;
    }

    @Override
    public byte[] getValue() {
      return value;
    }

    @Override
    public byte[] setValue(byte[] value) {
      if (value == null) {
        throw new IllegalArgumentException("Value must not be null.");
      }
      byte[] oldValue = this.value;
      this.value = value;
      return oldValue;
    }

    /**
     * This compares Map.Entry but not the way Map.Entry says we should compare two entries, because that does not work well for byte arrays.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof Map.Entry)) {
        return false;
      }

      Map.Entry e1 = this;
      Map.Entry e2 = (Map.Entry) o;

      return (e1.getKey() == null ?
          e2.getKey() == null : e1.getKey().equals(e2.getKey()))  &&
          (e1.getValue() == null ?
              e2.getValue() == null : Arrays.equals((byte[])e1.getValue(), (byte[])e2.getValue()));
    }

    /**
     * This computes a hashCode() the not the way way Map.Entry says we should compute hashCode, as that does not work well for byte arrays.
     */
    @Override
    public int hashCode() {
      return (getKey() == null   ? 0 : getKey().hashCode() ^
          (getValue() == null ? 0 : Arrays.hashCode(getValue())));
    }
  };

  private List<Map.Entry<String, byte[]>> _backingList;

  private ByteBuffer _headerSource;

  public LazyHeaderListMap() {
    this((ByteBuffer) null);
  }

  public LazyHeaderListMap(Map<String, byte[]> other) {
    if (other instanceof LazyHeaderListMap) {
      LazyHeaderListMap otherLazyHeaderListMap = (LazyHeaderListMap) other;
      otherLazyHeaderListMap.lazyInit();
      this._backingList = new ArrayList<>(otherLazyHeaderListMap._backingList);
    } else {
      this._backingList = new ArrayList<>(other.size());
      this._backingList.addAll(other.entrySet());
    }
  }

  /**
   * When the map is accessed then _headerSource is parsed.
   *
   * @param headerSource  this may be null in which case there are not any headers.
   */
  public LazyHeaderListMap(ByteBuffer headerSource) {
    this._headerSource = headerSource;
  }

  private void lazyInit() {
    if (_backingList != null) {
      return;
    }
    if (_headerSource == null) {
      //Not using empty list so that this map remains mutable
      _backingList = new ArrayList<>(0);
      return;
    }

    _backingList = new ArrayList<>();
    BiConsumer<String, byte[]> initializer = (k, v) -> _backingList.add(new Entry(k, v));
    DefaultHeaderDeserializer.parseHeader(_headerSource, initializer);
    _headerSource = null;

    assert checkDuplicates();
  }

  /**
   * This checks for duplicates in the map.  If you are are calling this function they you have givenup on performance
   * just use a HashMap.  Otherwise only call this for testing , debugging or in an assert.
   * @return true if we are duplicate free.
   */
  private boolean checkDuplicates() {
    Set<String> keysSeen = new HashSet<>(_backingList.size() * 2);
    for (Map.Entry<String, byte[]> entry  : _backingList) {
      if (keysSeen.contains(entry.getKey())) {
        return false;
      }
      keysSeen.add(entry.getKey());
    }
    return true;
  }

  @Override
  public int size() {
    lazyInit();

    return _backingList.size();
  }

  @Override
  public boolean isEmpty() {
    lazyInit();
    return _backingList.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    lazyInit();
    for (int i = _backingList.size() - 1; i >= 0; i--) {
      if (_backingList.get(i).getKey().equals(key)) {
        return true;
      }
    }
    return false;
  }

  /**
   *  This is not supported.
   */
  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] get(Object key) {
    lazyInit();

    for (int i = 0;  i < _backingList.size(); i++) {
      if (_backingList.get(i).getKey().equals(key)) {
        return _backingList.get(i).getValue();
      }
    }
    return null;
  }

  /**
   *  This has O(n) run time as we check for duplicates when this is added.
   *
   * @param key non-null
   * @param value non-null
   * @return  The previous value stored with the key or null if there was no such value.
   */
  @Override
  public byte[] put(String key, byte[] value) {
    lazyInit();

    if (key == null) {
      throw new IllegalArgumentException("null keys are not supported.");
    }
    if (value == null) {
      throw new IllegalArgumentException("null values are not supported.");
    }

    for (int i = 0; i < _backingList.size(); i++) {
      if (_backingList.get(i).getKey().equals(key)) {
        byte[] previousValue = _backingList.get(i).getValue();
        _backingList.set(i, new Entry(key, value));
        return previousValue;
      }
    }
    _backingList.add(new Entry(key, value));
    return null;
  }

  /**
   * This is an O(n) operation.
   *
   * @param key non-null
   * @return null if the key did not exist.
   */
  @Override
  public byte[] remove(Object key) {
    lazyInit();

    if (key == null) {
      throw new IllegalStateException("key must not be null");
    }
    for (int i = 0; i < _backingList.size(); i++) {
      if (_backingList.get(i).getKey().equals(key)) {
        int lastIndex = _backingList.size() - 1;
        byte[] value = _backingList.get(i).getValue();
        _backingList.set(i, _backingList.get(lastIndex));
        _backingList.remove(lastIndex);
        return value;
      }
    }
    return null;
  }

  @Override
  public void putAll(Map<? extends String, ? extends byte[]> m) {
    for (Map.Entry<? extends String, ? extends byte[]> otherEntry : m.entrySet()) {
      put(otherEntry.getKey(), otherEntry.getValue());
    }
  }

  @Override
  public void clear() {
    lazyInit();
    _backingList = new ArrayList<>(0);
  }

  @Override
  public Set<String> keySet() {
    lazyInit();

    return new Set<String>() {

      @Override
      public int size() {
        return _backingList.size();
      }

      @Override
      public boolean isEmpty() {
        return _backingList.isEmpty();
      }

      @Override
      public boolean contains(Object o) {
        return containsKey(o);
      }

      @Override
      public Iterator<String> iterator() {

        return new Iterator<String>() {
          final int backingListSizeAtInitializationTime = _backingList.size();
          int nextIndex = 0;

          @Override
          public boolean hasNext() {
            if (_backingList.size() != backingListSizeAtInitializationTime) {
              throw new ConcurrentModificationException();
            }

            return nextIndex < _backingList.size();
          }

          @Override
          public String next() {
            if (_backingList.size() != backingListSizeAtInitializationTime) {
              throw new ConcurrentModificationException();
            }
            return _backingList.get(nextIndex++).getKey();
          }
        };
      }

      @Override
      public Object[] toArray() {
        return _backingList.toArray(new Object[_backingList.size()]);
      }

      @Override
      public <T> T[] toArray(T[] a) {
        return _backingList.toArray(a);
      }

      @Override
      public boolean add(String s) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean remove(Object o) {
        return LazyHeaderListMap.this.remove(o) != null;
      }

      @Override
      public boolean containsAll(Collection<?> c) {
        for (Object key : c) {
          if (!containsKey(key)) {
            return false;
          }
        }
        return true;
      }

      @Override
      public boolean addAll(Collection<? extends String> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean removeAll(Collection<?> c) {
        boolean setChanged = false;
        for (Object key : c) {
          setChanged = setChanged || remove(key);
        }

        return setChanged;
      }

      @Override
      public void clear() {
        _backingList.clear();
      }
    };
  }

  @Override
  public Collection<byte[]> values() {
    lazyInit();
    return new Collection<byte[]>() {

      @Override
      public int size() {
        return _backingList.size();
      }

      @Override
      public boolean isEmpty() {
        return _backingList.isEmpty();
      }

      @Override
      public boolean contains(Object o) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Iterator<byte[]> iterator() {
        return new Iterator<byte[]>() {
          final int backingListSizeAtInitializationTime = _backingList.size();
          private int index = 0;

          @Override
          public boolean hasNext() {
            if (backingListSizeAtInitializationTime != _backingList.size()) {
              throw new ConcurrentModificationException();
            }
            return index < _backingList.size();
          }

          @Override
          public byte[] next() {
            if (backingListSizeAtInitializationTime != _backingList.size()) {
              throw new ConcurrentModificationException();
            }

            return _backingList.get(index++).getValue();
          }
        };
      }

      @Override
      public Object[] toArray() {
        throw new UnsupportedOperationException();
      }

      @Override
      public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean add(byte[] bytes) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean remove(Object o) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean addAll(Collection<? extends byte[]> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public Set<Map.Entry<String, byte[]>> entrySet() {
    lazyInit();

    return new Set<Map.Entry<String, byte[]>>() {

      @Override
      public int size() {
        return LazyHeaderListMap.this.size();
      }

      @Override
      public boolean isEmpty() {
        return LazyHeaderListMap.this.isEmpty();
      }

      @Override
      public boolean contains(Object o) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Iterator<Map.Entry<String, byte[]>> iterator() {
        return _backingList.iterator();
      }

      @Override
      public Object[] toArray() {
        throw new UnsupportedOperationException();
      }

      @Override
      public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean add(Map.Entry<String, byte[]> stringEntry) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean remove(Object o) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean addAll(Collection<? extends Map.Entry<String, byte[]>> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
        throw new UnsupportedOperationException();
      }
    }; // Set<Map.Entry<String, byte[]>>
  }

  @Override
  public boolean equals(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }
}
