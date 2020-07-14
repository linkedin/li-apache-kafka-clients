/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.clients.security;

import com.linkedin.kafka.clients.security.errors.SecurityException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DefaultMessageEncrypterDecrypterTest {

  @Test
  public void testCorrectness() {
    MessageEncrypterDecrypter encrypterDecrypter = new DefaultMessageEncrypterDecrypter();

    String text = "message";
    byte[] encryptedText = encrypterDecrypter.encrypt(text.getBytes());
    // the expected length of an array of n bytes encoded by Base64 is 4 * ceil(n/3)
    Assert.assertEquals(4 * ((int) Math.ceil(text.length() / 3.0)), encryptedText.length);

    byte[] decryptedText = encrypterDecrypter.decrypt(encryptedText);
    Assert.assertEquals(text.getBytes(), decryptedText);
  }

  @Test(expectedExceptions = SecurityException.class)
  public void testBadEncryptionMessage() {
    MessageEncrypterDecrypter encrypterDecrypter = new DefaultMessageEncrypterDecrypter();
    byte[] encryptedText = new byte[]{0, 0, 0, 0}; // not valid Base64 encoding result
    byte[] res = encrypterDecrypter.decrypt(encryptedText);
  }
}
