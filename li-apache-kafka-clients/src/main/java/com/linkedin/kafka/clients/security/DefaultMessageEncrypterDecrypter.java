/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.clients.security;
import com.linkedin.kafka.clients.security.errors.SecurityException;
import java.util.Base64;

public class DefaultMessageEncrypterDecrypter implements MessageEncrypterDecrypter {

  private final String _key;

  public DefaultMessageEncrypterDecrypter(String topic) {
    _key = topic;
  }

  public DefaultMessageEncrypterDecrypter() {
    _key = "defaultKey";
  }

  @Override
  public byte[] encrypt(byte[] plainText) throws SecurityException {
    return Base64.getEncoder().encode(plainText);
  }

  @Override
  public byte[] decrypt(byte[] cipherText) throws SecurityException {
    try {
      return Base64.getDecoder().decode(cipherText);
    } catch (IllegalArgumentException e) {
      throw new SecurityException("Unable to decrypt the cipher with Base64", e);
    }

  }
}
