/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.clients.encryption;

import com.linkedin.kafka.clients.encryption.errors.EncryptionException;


public interface MessageEncrypterDecrypter {
  /**
   * Encrypt the plaintext provided as a byte array
   *
   * @param plainText byte[] representing the payload to be encrypted
   * @return byte[] representing the encrypted payload
   * @throws EncryptionException Thrown if the payload could not be encrypted or encryption fails
   */
  byte[] encrypt(byte[] plainText) throws EncryptionException;

  /**
   * Decrypt the ciphertext provided as a byte array
   *
   * @param cipherText byte[] representing the encrypted payload that needs to be decrypted
   * @return byte[] representing the decrypted payload
   * @throws EncryptionException   Thrown if the payload could not be decrypted or decryption fails
   */
  byte[] decrypt(byte[] cipherText) throws EncryptionException;
}
