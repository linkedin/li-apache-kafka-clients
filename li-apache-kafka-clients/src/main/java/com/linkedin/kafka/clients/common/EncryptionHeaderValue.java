/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.clients.common;

/**
 * This class represents the header value for a encrypted message.
 * Currently, it contains only one field:
 *
 * | type   |
 * | 1 byte    |
 *
 *
 * The encryption header values will be used to support encryption eventually.
 * (as opposed to implementing encryption based on large message support)
 */
public class EncryptionHeaderValue {

  // Encryption in V1 version will not encrypt/decrypt data
  // which is used for compatibility with old consumer using
  // LM Segment based encryption
  public static final byte V1 = 0;
  // Encryption in V2 will encrypt/decrypt data as we expect
  public static final byte V2 = 1;
  public static final byte CURRENT_VERSION = V1;
  private static final int ENCRYPTION_HEADER_V1_SIZE = 1;
  private final byte _type;

  public EncryptionHeaderValue(byte type) {
    this._type = type;
  }

  public static byte[] toBytes(EncryptionHeaderValue encryptionHeaderValue) {
    byte[] serialized = new byte[ENCRYPTION_HEADER_V1_SIZE];
    int byteOffset = 0;
    serialized[byteOffset] = encryptionHeaderValue.getType();
    return serialized;
  }

  public static EncryptionHeaderValue fromBytes(byte[] bytes) {
    int byteOffset = 0;
    byte type = bytes[byteOffset];
    return new EncryptionHeaderValue(type);
  }

  public byte getType() {
    return _type;
  }
}
