package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.headers.DefaultHeaderDeserializer;
import com.linkedin.kafka.clients.headers.HeaderDeserializer.DeserializeResult;
import com.linkedin.kafka.clients.headers.HeaderUtils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


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

    Version0Deseriaizer deseriaizer = new Version0Deseriaizer(new DefaultHeaderDeserializer());
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

  private byte[] version0LargeMessageValue(byte[] testValueBytes, UUID messageId, int sequenceNumber,
      int numberOfSegments, int valueSizeInBytes) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(1 + Version0Deseriaizer.SEGMENT_INFO_OVERHEAD +
        testValueBytes.length + Version0Deseriaizer.CHECKSUM_LENGTH);
    byteBuffer.put(Version0Deseriaizer.LARGE_MESSAGE_ONLY_VERSION);
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
