/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.utils.DefaultHeaderSerializerDeserializer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 *  This is a specialized map backed by a list instead of a more asymptotically efficient data structure as our
 *  benchmarks show that for small numbers of items this will be more efficient than using HashMap.
 *
 */
public class LazyHeaderListMap implements Map<Integer, byte[]> {


  private static final class Entry implements Map.Entry<Integer, byte[]> {
    private final Integer key;
    private final byte[] value;

    public Entry(Integer key, byte[] value) {
      if (key == null) {
        throw new NullPointerException("key must not be null");
      }
      if (value == null) {
        throw new NullPointerException("value must not be null");
      }

      this.key = key;
      this.value = value;
    }

    @Override
    public Integer getKey() {
      return key;
    }

    @Override
    public byte[] getValue() {
      return value;
    }

    @Override
    public byte[] setValue(byte[] value) {
      throw new UnsupportedOperationException();
    }
  };

  private List<Map.Entry<Integer, byte[]>> backingList;

  private ByteBuffer headerSource;

  public LazyHeaderListMap() {
    this((ByteBuffer) null);
  }

  public LazyHeaderListMap(Map<Integer, byte[]> other) {
    if (other instanceof LazyHeaderListMap) {
      LazyHeaderListMap otherLazyHeaderListMap = (LazyHeaderListMap) other;
      otherLazyHeaderListMap.lazyInit();
      this.backingList = new ArrayList<>(otherLazyHeaderListMap.backingList);
    } else {
      this.backingList = new ArrayList<>(other.size());
      this.backingList.addAll(other.entrySet());
    }
  }

  /**
   * When the map is accessed then headerSource is parsed.
   *
   * @param headerSource  this may be null in which case there are not any headers.
   */
  public LazyHeaderListMap(ByteBuffer headerSource) {
    this.headerSource = headerSource;
  }

  private void lazyInit() {
    if (backingList != null) {
      return;
    }
    if (headerSource == null) {
      //Not using empty list so that this map remains mutable
      backingList = new ArrayList<>(0);
      return;
    }
    backingList = new ArrayList<>();
    DefaultHeaderSerializerDeserializer.parseHeader(headerSource, this);
    headerSource = null;
  }

  @Override
  public int size() {
    lazyInit();
    //This does not check for duplicates
    return backingList.size();
  }

  @Override
  public boolean isEmpty() {
    lazyInit();
    return backingList.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    lazyInit();
    for (int i = backingList.size() - 1; i >= 0; i--) {
      if (backingList.get(i).getKey().equals(key)) {
        return true;
      }
    }
    return false;
  }

  /**
   * This is unsupported because the same key can appear twice in the backing list which means we would need to
   * know if the key which this was associated with was the last key added.
   *
   * @param value
   * @return
   */
  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] get(Object key) {
    lazyInit();
    //searching backwards means we get the last value that was put for some key
    for (int i = backingList.size() - 1; i >= 0; i--) {
      if (backingList.get(i).getKey().equals(key)) {
        return backingList.get(i).getValue();
      }
    }
    return null;
  }

  /**
   *
   * @param key non-null
   * @param value non-null
   * @return  This always returns null
   */
  @Override
  public byte[] put(Integer key, byte[] value) {
    lazyInit();

    if (key == null) {
      throw new IllegalArgumentException("null keys are not supported.");
    }
    if (value == null) {
      throw new IllegalArgumentException("null values are not supported.");
    }
    backingList.add(new Entry(key, value));
    return null; //this breaks map spec
  }

  @Override
  public byte[] remove(Object key) {
    lazyInit();
    byte[] mapping = null;
    for (int i = 0; i < backingList.size(); i++) {
      if (backingList.get(i).getKey().equals(key)) {
        mapping = backingList.remove(i).getValue();
        i--;
      }
    }
    return mapping;
  }

  @Override
  public void putAll(Map<? extends Integer, ? extends byte[]> m) {
    for (Map.Entry<? extends Integer, ? extends byte[]> otherEntry : m.entrySet()) {
      put(otherEntry.getKey(), otherEntry.getValue());
    }
  }

  @Override
  public void clear() {
    lazyInit();
    backingList = new ArrayList<>(0);
  }

  /**
   * The set returned by this method may not actually be a set in that it can contain duplicates.
   *
   * @return non-null
   */
  @Override
  public Set<Integer> keySet() {
    lazyInit();

    return new Set<Integer>() {

      @Override
      public int size() {
        return backingList.size();
      }

      @Override
      public boolean isEmpty() {
        return backingList.isEmpty();
      }

      @Override
      public boolean contains(Object o) {
        return containsKey(o);
      }

      @Override
      public Iterator<Integer> iterator() {

        return new Iterator<Integer>() {
          final int backingListSizeAtInitializationTime = backingList.size();
          int nextIndex = 0;

          @Override
          public boolean hasNext() {
            if (backingList.size() != backingListSizeAtInitializationTime) {
              throw new ConcurrentModificationException();
            }

            return nextIndex < backingList.size();
          }

          @Override
          public Integer next() {
            if (backingList.size() != backingListSizeAtInitializationTime) {
              throw new ConcurrentModificationException();
            }
            return backingList.get(nextIndex++).getKey();
          }
        };
      }

      @Override
      public Object[] toArray() {
        return backingList.toArray(new Object[backingList.size()]);
      }

      @Override
      public <T> T[] toArray(T[] a) {
        return backingList.toArray(a);
      }

      @Override
      public boolean add(Integer integer) {
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
      public boolean addAll(Collection<? extends Integer> c) {
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
        backingList.clear();
      }
    };
  }

  /**
   * Not implemented.
   *
   * @return
   */
  @Override
  public Collection<byte[]> values() {
    lazyInit();
    return new Collection<byte[]>() {

      @Override
      public int size() {
        return backingList.size();
      }

      @Override
      public boolean isEmpty() {
        return backingList.isEmpty();
      }

      @Override
      public boolean contains(Object o) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Iterator<byte[]> iterator() {
        return new Iterator<byte[]>() {
          final int backingListSizeAtInitializationTime = backingList.size();
          private int index = 0;

          @Override
          public boolean hasNext() {
            if (backingListSizeAtInitializationTime != backingList.size()) {
              throw new ConcurrentModificationException();
            }
            return index < backingList.size();
          }

          @Override
          public byte[] next() {
            if (backingListSizeAtInitializationTime != backingList.size()) {
              throw new ConcurrentModificationException();
            }

            return backingList.get(index++).getValue();
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

  /**
   * Not implemented.
   *
   */
  @Override
  public Set<Map.Entry<Integer, byte[]>> entrySet() {
    throw new UnsupportedOperationException();
  }

  /**
   * This is problematic to implement if some kind of duplicate elimination is not done on the backingList.
   */
  @Override
  public boolean equals(Object o) {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * This is problematic to implement if some kind of duplicate elimination is not done on the backingList.
   */
  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }
}
