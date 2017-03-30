package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.headers.DefaultHeaderDeserializer;
import com.linkedin.kafka.clients.headers.HeaderDeserializer;
import com.linkedin.kafka.clients.headers.HeaderUtils;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Attempts to deserialize the current version and then the previous version that did not use headers for large messages.
 * <pre>
 * The large message segment serializer.
 * The format of the serialized segment is:
 * 1 byte   - version
 * 4 bytes  - checksum to determine if bytes is large message segment or not.
 * 16 bytes - messageId
 * 4 bytes  - sequence number
 * 4 bytes  - number of segments
 * 4 bytes  - message size in bytes
 * X bytes  - payload
 * </pre>
 */
public class Version0Deserializer implements HeaderDeserializer {

  private final static Logger LOG = LoggerFactory.getLogger(Version0Deserializer.class);
  static final int SEGMENT_INFO_OVERHEAD = 16 + Integer.BYTES + Integer.BYTES + Integer.BYTES;
  static final byte LARGE_MESSAGE_ONLY_VERSION = 0;
  static final int CHECKSUM_LENGTH = Integer.BYTES;

  private HeaderDeserializer _tryFirst;

  public Version0Deserializer(HeaderDeserializer tryFirst) {
    if (tryFirst == null) {
      throw new IllegalArgumentException("This deserializer only supports legacy serilized values.  No new values of"
          + " this type are supported.");
    }
    _tryFirst = tryFirst;
  }

  public Version0Deserializer() {
    this(new DefaultHeaderDeserializer());
  }

  @Override
  public DeserializeResult deserializeHeader(ByteBuffer src) {
    DeserializeResult deserializeResult = _tryFirst.deserializeHeader(src);
    if (deserializeResult.success()) {
      return deserializeResult;
    }

    if (src == null) {
      return new DeserializeResult(null, null, true);
    }

    //Attempt to deserialize version 0 headers
    int headerLength = 1 + SEGMENT_INFO_OVERHEAD + CHECKSUM_LENGTH;
    if (src.remaining() < headerLength) {
      // Serialized segment size too small, not large message segment.
      return new DeserializeResult(null, src, false);
    }
    int originalSrcPosition = src.position();
    byte version = src.get();
    if (version != LARGE_MESSAGE_ONLY_VERSION) {
      LOG.debug("Serialized version byte is not than {}, not large message segment.",
          LARGE_MESSAGE_ONLY_VERSION);
      src.position(originalSrcPosition);
      return new DeserializeResult(null, src, false);
    }
    int checksum = src.getInt();
    long messageIdMostSignificantBits = src.getLong();
    long messageIdLeastSignificantBits = src.getLong();
    if (checksum == 0 ||
        checksum != ((int) (messageIdMostSignificantBits + messageIdLeastSignificantBits))) {
      LOG.debug("Serialized segment checksum does not match, not a large message segment.");
      src.position(originalSrcPosition);
      return new DeserializeResult(null, src, false);
    }

    UUID messageId = new UUID(messageIdMostSignificantBits, messageIdLeastSignificantBits);
    int sequenceNumber = src.getInt();
    int numberOfSegments = src.getInt();
    int messageSizeInBytes = src.getInt();
    LargeMessageSegment currentVersionOfLargeMessageSegment =
        new LargeMessageSegment(messageId, sequenceNumber, numberOfSegments, messageSizeInBytes, false, src);
    Map<String, byte[]> headers = new HashMap<>(1);
    headers.put(HeaderUtils.LARGE_MESSAGE_SEGMENT_HEADER, currentVersionOfLargeMessageSegment.segmentHeader());
    ByteBuffer payload = src.slice();
    return new DeserializeResult(headers, payload, true);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _tryFirst.configure(configs);
  }

}
