/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.clients.security;

import com.linkedin.kafka.clients.security.errors.SecurityException;

/**
 * Interface to handle basic byte encryption and decryption
 *
 * The implementing interface is expected to manage the encryption context and/or encryption keys, as appropriate.
 */
public interface KafkaMessageEncrypterDecrypter {
  /**
   * Encrypt the plaintext provided as a byte array
   *
   * @param plainText byte[] representing the payload to be encrypted
   * @return byte[] representing the encrypted payload
   * @throws SecurityException Thrown if the payload could not be encrypted or encryption fails
   */
  byte[] encrypt(byte[] plainText) throws SecurityException;

  /**
   * Decrypt the ciphertext provided as a byte array
   *
   * @param cipherText byte[] representing the encrypted payload that needs to be decrypted
   * @return byte[] representing the decrypted payload
   * @throws SecurityException   Thrown if the payload could not be decrypted or decryption fails
   */
  byte[] decrypt(byte[] cipherText) throws SecurityException;
}
