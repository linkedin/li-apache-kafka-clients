/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

/**
 * Class to encode/decode primitive integer/long types.
 */
public class PrimitiveEncoderDecoder {
  // The number of bytes for a long variable
  public static final int LONG_SIZE = Long.SIZE / Byte.SIZE;
  public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

  /**
   * Avoid instantiating PrimitiveEncoderDecoder class
   */
  private PrimitiveEncoderDecoder() {

  }

  /**
   * Encodes a long value into a {@link PrimitiveEncoderDecoder#LONG_SIZE} byte array
   */
  public static void encodeLong(long value, byte[] output, int pos) {
    if (output == null) {
      throw new IllegalArgumentException("The input result cannot be null");
    }

    if (pos < 0) {
      throw new IllegalArgumentException("position cannot be less than zero");
    }

    if (output.length < pos + LONG_SIZE) {
      throw new IllegalArgumentException(
          String.format("Not adequate bytes available to encode the long value(array length = %d, pos = %d", output.length, pos)
      );
    }

    for (int i = LONG_SIZE - 1; i >= 0; i--) {
      output[pos + i] = (byte) (value & 0xffL);
      value >>= 8;
    }
  }

  /**
   * Encodes a long value into a newly created byte[] and returns it.
   */
  public static byte[] encodeLong(long value) {
    byte[] data = new byte[LONG_SIZE];
    encodeLong(value, data, 0);
    return data;
  }

  /**
   * Encodes a int value into a {@link PrimitiveEncoderDecoder#INT_SIZE} byte array
   */
  public static void encodeInt(int value, byte[] output, int pos) {
    if (output == null) {
      throw new IllegalArgumentException("The input result cannot be null");
    }

    if (pos < 0) {
      throw new IllegalArgumentException("position cannot be less than zero");
    }

    if (output.length < pos + INT_SIZE) {
      throw new IllegalArgumentException(
          String.format("Not adequate bytes available to encode the int value(array length = %d, pos = %d", output.length, pos)
      );
    }

    output[pos] = (byte) (value >> 24);
    output[pos + 1] = (byte) (value >> 16);
    output[pos + 2] = (byte) (value >> 8);
    output[pos + 3] = (byte) value;
  }

  /**
   * Encodes a int value int a newly created byte[] and returns it
   */
  public static byte[] encodeInt(int value) {
    byte[] data = new byte[INT_SIZE];
    encodeInt(value, data, 0);
    return data;
  }

  public static int decodeInt(byte[] input, int pos) {
    if (input == null) {
      throw new IllegalArgumentException("bytes cannot be null");
    }

    if (pos < 0) {
      throw new IllegalArgumentException("position cannot be less than zero");
    }

    if (input.length < pos + INT_SIZE) {
      throw new IllegalArgumentException(
          String.format("Not adequate bytes available in the input array(array length = %d, pos = %d)", input.length, pos)
      );
    }

    return input[pos] << 24 | (input[pos + 1] & 0xFF) << 16 | (input[pos + 2] & 0xFF) << 8 | (input[pos + 3] & 0xFF);
  }

  /**
   * Decodes {@link PrimitiveEncoderDecoder#LONG_SIZE} bytes from offset in the input byte array
   */
  public static long decodeLong(byte[] input, int pos) {
    if (input == null) {
      throw new IllegalArgumentException("bytes cannot be null");
    }

    if (pos < 0) {
      throw new IllegalArgumentException("position cannot be less than zero");
    }

    if (input.length < pos + LONG_SIZE) {
      throw new IllegalArgumentException(
          String.format("Not adequate bytes available in the input array(array length = %d, pos = %d)", input.length, pos)
      );
    }

    return (input[pos] & 0xFFL) << 56
        | (input[pos + 1] & 0xFFL) << 48
        | (input[pos + 2] & 0xFFL) << 40
        | (input[pos + 3] & 0xFFL) << 32
        | (input[pos + 4] & 0xFFL) << 24
        | (input[pos + 5] & 0xFFL) << 16
        | (input[pos + 6] & 0xFFL) << 8
        | (input[pos + 7] & 0xFFL);
  }
}