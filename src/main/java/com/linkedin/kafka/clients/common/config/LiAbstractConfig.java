package com.linkedin.kafka.clients.common.config;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

public abstract class LiAbstractConfig extends AbstractConfig {
  public LiAbstractConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
    super(definition, originals, doLog);
  }

  public LiAbstractConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
  }

  public LiAbstractConfig(Map<String, Object> parsedConfig) {
    super(parsedConfig);
  }

  /**
   * Get a instance of the give class specified by the given configuration key.
   */
  public <T> T getInstance(String key, Class<T> t) {
    Class<?> c = getClass(key);
    if (c == null)
      return null;
    Object o = Utils.newInstance(c);
    if (!t.isInstance(o))
      throw new KafkaException(c.getName() + " is not an instance of " + t.getName());
    return t.cast(o);
  }
}
