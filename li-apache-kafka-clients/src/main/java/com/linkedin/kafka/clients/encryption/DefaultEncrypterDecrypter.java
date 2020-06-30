/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.clients.encryption;

import com.linkedin.kafka.clients.encryption.errors.EncryptionException;
import java.util.Base64;


public class DefaultEncrypterDecrypter implements MessageEncrypterDecrypter {
  @Override
  public byte[] encrypt(byte[] plainText) throws EncryptionException {
    return Base64.getEncoder().encode(plainText);
  }

  @Override
  public byte[] decrypt(byte[] cipherText) throws EncryptionException {
    try {
      return Base64.getDecoder().decode(cipherText);
    } catch (IllegalArgumentException e) {
      throw new EncryptionException("Unable to decrypt the cipher with Base64", e);
    }

  }
}
