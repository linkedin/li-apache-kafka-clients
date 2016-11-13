package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.utils.HeaderParser;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 *  This is a specialized map backed by a list instead of a more asymptotocally efficient data structure as our
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

  private List<Entry> backingList;

  private ByteBuffer headerSource;

  /**
   * T
   * @param headerSource  this may be null in which case there are not any headers
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
    HeaderParser.parseHeader(headerSource, this);
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
    for (int i=backingList.size()-1; i >= 0; i--) {
      if (backingList.get(i).getKey().equals(key)) {
        return backingList.get(i).getValue();
      }
    }
    return null;
  }

  /**
   *
   * @param key
   * @param value
   * @return  This always returns null
   */
  @Override
  public byte[] put(Integer key, byte[] value) {
    lazyInit();
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
   * Not implemented.
   * @return
   */
  @Override
  public Set<Integer> keySet() {
    throw new UnsupportedOperationException();
  }

  /**
   * Not implemented.
   *
   * @return
   */
  @Override
  public Collection<byte[]> values() {
    throw new UnsupportedOperationException();
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
