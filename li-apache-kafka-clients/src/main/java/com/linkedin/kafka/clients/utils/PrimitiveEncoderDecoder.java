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
  public static final int BOOLEAN_SIZE = 1;

  /**
   * Avoid instantiating PrimitiveEncoderDecoder class
   */
  private PrimitiveEncoderDecoder() {

  }

  /**
   * Encodes a long value into a {@link PrimitiveEncoderDecoder#LONG_SIZE} byte array
   * @param value value to be encoded
   * @param output output where encoded data will be stored
   * @param pos position in output where value will be encoded starting from
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
   * @param value value to be encoded
   * @return encoded form of value
   */
  public static byte[] encodeLong(long value) {
    byte[] data = new byte[LONG_SIZE];
    encodeLong(value, data, 0);
    return data;
  }

  /**
   * Encodes a int value into a {@link PrimitiveEncoderDecoder#INT_SIZE} byte array
   * @param value value to be encoded
   * @param output destination to be encoded into
   * @param pos position in destination where encoded value will start
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
   * @param value value to be encoded
   * @return encoded value
   */
  public static byte[] encodeInt(int value) {
    byte[] data = new byte[INT_SIZE];
    encodeInt(value, data, 0);
    return data;
  }


  /**
   * Encodes a boolean value into a {@link PrimitiveEncoderDecoder#BOOLEAN_SIZE} byte array
   * @param value value to be encoded
   * @param output output where encoded data will be stored
   * @param pos position in output where value will be encoded starting from
   */
  public static void encodeBoolean(boolean value, byte[] output, int pos) {
    if (output == null) {
      throw new IllegalArgumentException("The input result cannot be null");
    }

    if (pos < 0) {
      throw new IllegalArgumentException("position cannot be less than zero");
    }

    if (output.length < pos + BOOLEAN_SIZE) {
      throw new IllegalArgumentException(
          String.format("Not adequate bytes available to encode the boolean value(array length = %d, pos = %d", output.length, pos)
      );
    }
    output[pos] = (byte) (value ? 1 : 0);
  }

  /**
   * Decodes {@link PrimitiveEncoderDecoder#LONG_SIZE} bytes from offset in the input byte array
   * @param input where to read encoded form from
   * @param pos position in input to start reading from
   * @return a decoded long
   */
  public static long decodeLong(byte[] input, int pos) {
    sanityCheck(input, pos, LONG_SIZE);

    return (input[pos] & 0xFFL) << 56
        | (input[pos + 1] & 0xFFL) << 48
        | (input[pos + 2] & 0xFFL) << 40
        | (input[pos + 3] & 0xFFL) << 32
        | (input[pos + 4] & 0xFFL) << 24
        | (input[pos + 5] & 0xFFL) << 16
        | (input[pos + 6] & 0xFFL) << 8
        | (input[pos + 7] & 0xFFL);
  }


  /**
   * Decodes {@link PrimitiveEncoderDecoder#INT_SIZE} bytes from offset in the input byte array
   * @param input where to read encoded form from
   * @param pos position in input to start reading from
   * @return a decoded int
   */
  public static int decodeInt(byte[] input, int pos) {
    sanityCheck(input, pos, INT_SIZE);

    return input[pos] << 24 | (input[pos + 1] & 0xFF) << 16 | (input[pos + 2] & 0xFF) << 8 | (input[pos + 3] & 0xFF);
  }

  private static void sanityCheck(byte[] input, int pos, int dataSize) {
    if (input == null) {
      throw new IllegalArgumentException("bytes cannot be null");
    }

    if (pos < 0) {
      throw new IllegalArgumentException("position cannot be less than zero");
    }

    if (input.length < pos + dataSize) {
      throw new IllegalArgumentException(
          String.format("Not adequate bytes available in the input array(array length = %d, pos = %d)", input.length, pos)
      );
    }
  }
}