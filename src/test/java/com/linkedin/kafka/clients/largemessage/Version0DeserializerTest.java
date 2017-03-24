package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.headers.DefaultHeaderDeserializer;
import com.linkedin.kafka.clients.headers.DefaultHeaderSerializer;
import com.linkedin.kafka.clients.headers.HeaderDeserializer.DeserializeResult;
import com.linkedin.kafka.clients.headers.HeaderUtils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests that we can deserialize old encodings.
 */
public class Version0DeserializerTest {


  @Test
  public void testDeserializeVersion0Headers() {
    byte[] segmentBytes = new byte[1024];
    Arrays.fill(segmentBytes, (byte) 0x72);
    UUID messageId = UUID.randomUUID();
    int sequenceNumber = 3;
    int numberOfSements = 8;
    int valueSizeInBytes = segmentBytes.length * numberOfSements;
    byte[] version0LargeMessage = version0LargeMessageValue(segmentBytes, messageId, sequenceNumber, numberOfSements, valueSizeInBytes);

    Version0Deserializer deseriaizer = new Version0Deserializer();
    DeserializeResult deserializeResult = deseriaizer.deserializeHeader(ByteBuffer.wrap(version0LargeMessage));

    assertEquals(deserializeResult.value().array(), version0LargeMessage);
    byte[] largeMessageSegmentHeader = deserializeResult.headers().get(HeaderUtils.LARGE_MESSAGE_SEGMENT_HEADER);
    LargeMessageSegment largeMessageSegment = new LargeMessageSegment(largeMessageSegmentHeader, deserializeResult.value());
    assertEquals(largeMessageSegment.numberOfSegments(), numberOfSements);
    assertEquals(largeMessageSegment.sequenceNumber(), sequenceNumber);
    assertEquals(largeMessageSegment.originalValueSize(), valueSizeInBytes);
    assertEquals(largeMessageSegment.messageId(), messageId);
    assertEquals(largeMessageSegment.originalKeyWasNull(), false);
  }

  @Test
  public void testVersion0DeserializerWithHeaders() {
    DefaultHeaderSerializer serializer = new DefaultHeaderSerializer();
    Map<String, byte[]> headers = new HashMap<>();
    headers.put("header-key", new byte[] {-9});
    byte[] value = new byte[77];
    Arrays.fill(value, (byte)-9);
    ByteBuffer src = ByteBuffer.wrap(value);
    byte[] serialized = serializer.serializeHeaderWithValue(headers, src);

    Version0Deserializer deseriaizer = new Version0Deserializer();
    DeserializeResult deserializeResult = deseriaizer.deserializeHeader(ByteBuffer.wrap(serialized));
    assertTrue(deserializeResult.success());
    assertEquals(deserializeResult.headers(), headers);
    byte[] deserializedValue = new byte[deserializeResult.value().remaining()];
    deserializeResult.value().get(deserializedValue);
    assertEquals(deserializedValue, value);
  }

  private byte[] version0LargeMessageValue(byte[] testValueBytes, UUID messageId, int sequenceNumber,
      int numberOfSegments, int valueSizeInBytes) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(1 + Version0Deserializer.SEGMENT_INFO_OVERHEAD +
        testValueBytes.length + Version0Deserializer.CHECKSUM_LENGTH);
    byteBuffer.put(Version0Deserializer.LARGE_MESSAGE_ONLY_VERSION);
    byteBuffer.putInt((int) (messageId.getMostSignificantBits() + messageId.getLeastSignificantBits()));
    byteBuffer.putLong(messageId.getMostSignificantBits());
    byteBuffer.putLong(messageId.getLeastSignificantBits());
    byteBuffer.putInt(sequenceNumber);
    byteBuffer.putInt(numberOfSegments);
    byteBuffer.putInt(valueSizeInBytes);
    byteBuffer.put(testValueBytes);
    return byteBuffer.array();
  }
}
