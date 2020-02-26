li-apache-kafka-clients
===================
[ ![CircleCI](https://circleci.com/gh/linkedin/li-apache-kafka-clients/tree/master.svg?style=svg) ](https://circleci.com/gh/linkedin/li-apache-kafka-clients/tree/master)
[ ![Download](https://api.bintray.com/packages/linkedin/maven/li-apache-kafka-clients/images/download.svg) ](https://bintray.com/linkedin/maven/li-apache-kafka-clients/_latestVersion)

### Introduction ###
li-apache-kafka-clients is a wrapper Kafka clients library built on top of vanilla Apache Kafka clients.

Apache Kafka has now become a very popular messaging system and is well known for its low latency, high throughput and durable messaging. At LinkedIn, we have built an [ecosystem around Kafka](https://engineering.linkedin.com/blog/2016/04/kafka-ecosystem-at-linkedin) to power our infrastructure. In our ecosystem, li-apache-kafka-clients library is a fundamental component for many functions such as auditing, data format standardization, large message handling, and so on.

li-apache-kafka-clients is designed to be fully compatible with Apache Kafka vanilla clients. Both LiKafkaProducer and LiKafkaConsumer implement the vanilla Kafka Producer and Consumer interface.

li-apache-kafka-clients is also highly customizable so the users can plug in their own wire protocol for large message segments and auditor implementation.

### Features ###
li-apache-kafka-clients has the following features in addition to the vanilla Apache Kafka Java clients.

#### Large message support ####
Like many other messaging systems, Kafka has a maximum message size limit for a few reasons (e.g. memory management. Due to the message size limit, in some cases, user have to bear with small amount of message loss or involve external storage to store the large objects. li-apache-kafka-clients addresses this problem with [a solution](http://www.slideshare.net/JiangjieQin/handle-large-messages-in-apache-kafka-58692297) that does not require an external storage dependency. For users who are storing offsets checkpoints in Kafka, li-apache-kafka-clients handles large messages almost transparently.

#### Auditing ####
As a messaging system, the data assurance is critical. At LinkedIn, we have been using a counting based out-of-band auditing solution to monitor the data integrity in our various data pipelines. This auditing solution is described in [these meetup talk slides](http://www.slideshare.net/JonBringhurst/kafka-audit-kafka-meetup-january-27th-2015). li-apache-kafka-clients has integrated a pluggable auditing feature. We have also provided an `AbstractAuditor` and a `CountingAuditStats` class to help user implement the counting based auditing solution similar to what we are using at LinkedIn. Users can group the messages in to various categories based on `AuditKey` and get the statistics for each `AuditKey`. For more details please refer to the Java doc of `AbstractAuditor` and `CountingAuditStats`. We have provided a `LoggingAuditor` class as an example of using `AbstractAuditor` to implement the counting based auditing.

Users may also have a custom auditing solution by implementing the defined `Auditor` interface.  

### When to use li-apache-kafka-clients ###
li-apache-kafka-clients is a wrapper library on top of vanilla Kafka Java clients. If one does not need the additional functions provided by li-apache-kafka-clients, the vanilla Kafka Java clients should be preferred.

### Adding the Repository to Your Build ###
```gradle
repositories {
  jcenter()
}

dependencies {
  compile 'com.linkedin.kafka.clients:li-apache-kafka-clients:1.0.39'
  testCompile 'com.linkedin.kafka.clients:kafka-test-harness:1.0.39' //if you want to use the test harness 
}
```

### Build jars and run all the unit tests ###
`./gradlew build`

### Handle large messages with li-apache-kafka-clients ###
Large messages are the messages whose size are greater than the maximum acceptable message size on the broker. li-apache-kafka-clients handles large messages by splitting each large message into segments in LiKafkaProducer and reassembling the segments back into the original large message in LiKafkaConsumer. 

li-apache-kafka-clients is designed to handle **sporadic** large messages. If the users expect to have a high percentage of large messages (e.g. >50%), the users may want to consider **reference-based-messaging** which is also described in the slides below.

The following sections are the user guide for applications to handle large messages with li-apache-kafka-clients. For more design details, users may refer to the slides about [Handle Large Messages in Apache Kafka](http://www.slideshare.net/JiangjieQin/handle-large-messages-in-apache-kafka-58692297).

#### Configurations for LiKafkaProducerImpl ####
`LiKafkaProducerImpl` takes the following large message related configurations:
```
large.message.enabled
max.message.segment.bytes
segment.serializer
```
If `large.message.enabled=true`, `LiKafkaProducerImpl` will split the messages whose serialized size is greater than `max.message.segment.bytes` into multiple `LargeMessageSegment`, serialize each `LargeMessageSegment` with the specified `segment.serializer` and send the serialized segments to Kafka brokers. 

* When `large.message.enabled=false` or a message is a normal sized message, `LiKafkaProducerImpl` will still send the message as a single-segment message. This is to make sure that the consumers will always be able to deserialize the messages regardless of the producer settings. However, with a custom `segment.serializer` users are able to use any wire protocols for the large message segments, including sending a normal-sized message without wrapping it with a large message segment.

* `LiKafkaProducerImpl` reuses `max.message.segment.bytes` as the threshold of large message. Users are expected to leave some headroom for the segment header and message headers (e.g. 50 KB).

* If user did not specify a `segment.serializer`, the `DefaultSegmentSerializer` will be used which uses a simple wire protocol. The `DefaultSegmentSerializer` uses 4 bytes derived from MessageId as the magic bytes. These 4 bytes will help the `DefaultSegmentDeserializer` to determine if the message is a large message segment.

* `LiKafkaProducerImpl` ensures the segments of the same large message will be sent to the same partition. If the original large message has a key, all the segments of that message will also share that key. If the original large message does not have a key, a randomly generated UUID will be used as the key.

`LiKafkaProducerImpl` allows user to plug in custom `segment.serializer` (e.g Avro). Users may use the custom segment serializer to use their own wire protocol for `LargeMessageSegment`. This may be useful for users who already have existing SerDe in the organization. For example, users may decide not to serialize a single-segment messages as a `LargeMessageSegment`, but simply send the payload of it which is the raw serialized message. Because the payload is the bytes serialized by value.serializer, this will maintain the existing wire protocol. Avoiding the segment size overhead may also be useful if most of the messages are small.

In `LiKafkaProducerConfig`, there is one **special configuration** `current.producer`. This configuration is not set by the users, but set by `LiKafkaProducerImpl` and passed to all the `Configurables` (key serializer, value serializer, segment serializer and the auditor). The value of this configuration is the underlying open source vanilla `KafkaProducer<byte[], byte[]>` used by LiKafkaProducerImpl. The motivation of having this configuration is to avoid creating separate producers in some cases. For example, at LinkedIn we send our auditing events back to an auditing topic in the same Kafka cluster the producer is sending messages to. With this underlying KafkaProducer, we don't need to create a separate producer to send the auditing events.

#### Configurations for LiKafkaConsumerImpl ####
`LiKafkaConsumerImpl` takes the following large message related configurations:
```
message.assembler.buffer.capacity
message.assembler.expiration.offset.gap
max.tracked.messages.per.partition
exception.on.message.dropped
segment.deserializer.class
```
`LiKafkaConsumerImpl` by default supports both large message and normal sized messages. There is no separate setting to enable large message support. `LiKafkaConsumerImpl` consumes the `ConsumerRecord` in raw bytes and uses a `ConsumerRecordsProcessor` to process the messages. The `ConsumerRecordsProcessor` buffers the large message segments and reassembles the large messages when all the segments of a large message are received. The messages buffered in the `ConsumerRecordsProcessor` are referred as ***incomplete large messages***. The total size of memory used to buffer the segments of incomplete large messages is upper-bounded by `message.assembler.buffer.capacity`. If the capacity is reached, `LiKafkaConsumerImpl` will drop the oldest incomplete message. Users can set `exception.on.message.dropped=true` if they want to receive an `LargeMessageDroppedException` when a incomplete large message is dropped.

In some cases, some buffered segments of large messages are never able to be assembled (e.g producer died before sending all the segments of a large message). If that happened, `LiKafkaConsumerImpl` will finally expire those large message segments to avoid memory leak. The expiration is based on the difference between current consumer offset of the partition and the **starting offset** (the offset of the first segment) of the incomplete large message. If the current consumer offset is greater than the starting offset of the incomplete large message + `message.assembler.expiration.offset.gap`, `LiKafkaConsumerImpl` will assume the large message will never be completed and drop all its buffered segments. In this case, the consumer **WILL NOT** throw an exception but consider this as a normal clean-up.

In order to support large message aware offset seek (see more in the ***About seek()*** section), `LiKafkaConsumerImpl` keeps track of the offsets of the messages that have been consumed for each partition. For efficiency, it only keeps track of the offsets of the messages if necessary. `max.tracked.messages.per.partition` specifies the maximum number of messages to track for each partition. The memory used to track each message is about 24 bytes. Depending on how frequent large messages appear and how many partitions the LiKafkaConsumer is consuming from, user may adjust this number to ensure it works for `seek()`. Typically there is no need to set a large value for this configuration unless users expect to `seek()` back aggressively.

Similar to `LiKafkaProducerImpl`, users can specify a custom `segment.deserializer` class to deserialize the bytes into a `LargeMessageSegment` object. In an organization with legacy SerDe implementation, users may have to support both large message segment and the legacy wire protocol of messages. The user implementation of segment deserializer should return `null` if it sees a message that is not a large message segment. `LiKafkaConsumerImpl` will then use the value deserializer to deserialize that message.

#### Consume large messages ####
`LiKafkaConsumerImpl` works almost transparently with large messages if the users do not need to seek to an arbitrary offset (See more in ***About seek()*** section). The potential challenging part is the offset management. We encourage users to use the methods provided by `LiKafkaConsumerImpl` to checkpoint the consumer offsets. By doing that the users do not need to deal with the nuances of large message offset management.

If the users only use `commitSync()`, `commitAsync()`, `seekToCommitted()` without specifying any particular offsets to commit or seek to, large message is completely transparent to the users.

If the users use `commitSync(OffsetMap)`, `commitAsync(OffsetMap)`, users have to make sure that the provided offsets to commit are (some actually consumed message offset + 1). This semantic is the same as vanilla open source consumer.

If the users use `seek()` to jump to an arbitrary offset, it is much more complicated. (See more in ***About seek()*** section).

For users who are interested in the details or want to manage the offsets on their own, it is useful to understand the following terms related to offsets:

* **Offset of a large message:** The ***offset of a large message*** is the offset of its ***last*** segment.
    *  Each large message segment has their own offset in the Kafka brokers
    *  The offsets of the segments other than the last segment are ***"invisible"*** to the users.
    * If user starts consumption from the middle of a large message, that large message will not be consumed.
* **Starting Offset of a large message:** The ***starting offset of a large message*** is the offset of its first segment.
    * This offset ensures that the large message can be reconsumed if users start consumption from there.
    * If we extend the definition to normal sized messages, the starting offset of a normal sized message is the the offset of that message.
* **Safe offset of a partition:** The ***safe offset of a partition*** is the smallest ***starting offset*** of all ***incomplete large messages*** in that partition at this moment.
    * This offset ensures that all the incomplete large messages at this point can be reconsumed if user starts consumption from there.
* **Safe offset of a message:** The ***safe offset of a message*** is the ***safe offset of the partition*** right after that message is delivered from that partition by the `ConsumreRecordsProcessor` to the user.
    * This offset allows users to commit the offset of a `ConsumerRecord` in the middle of a `ConsumerRecords`
    * This offset ensures that all the messages after that given message will be consumed if users starts consumption from there.
* **Consumer high watermark of a partition:** The ***consumer high watermark of a partition*** is the offset up to which  `LiKafkaConsumerImpl` has previously consumed from that partition. `LiKafkaConsumerImpl` will filter out the messages whose offset is less than the consumer high watermark of that partition. This is to avoid duplicate messages during rebalance or `seek()`. Notice that **consumer high watermark is different from the partition high watermark maintain the by the brokers**.
    * During offset commit, the consumer high watermark is the offset specified by the user when committing offsets. Or the last consumed message if the users did not provide specific offsets to commit.
    * During rebalance, the consumer high watermark is the offset of the last consumed message from a partition.
    * During `seek()`, the consumer high watermark is the offset the users seek to.

`LiKafkaConsumerImpl` manages the starting offset and safe offset for each messages it has consumed through a sparse map in the `DeliveredMessageOffsetTracker`. The safe offsets are also committed with the `OffsetMetadata` to Kafka. If users wants to manage the offsets outside of Kafka, the users can get the safe offsets through `safeOffset(TopicPartition)`, `safeOffset(TopicPartition, MessageOffset)` and `safeOffsets()` methods.

#### About seek() ####
Seeking to a given offset when large messages exist is challenging and tricky. **`LiKafkaConsumerImpl` supports large message aware `seek()` function but with some limitations**. `LiKafkaConsumerImpl` keeps track of the messages that have been consumed with a `DeliveredMessageOffsetTracker`. The `DeliveredMessageOffsetTracker` keeps track of all the messages that have been consumed for each partition. The seek function behaves differently based on the **tracked offsets range** (from the first consumed offset to the last consumed offset). The behavior and **limitations** of `seek()` are the following:

* If the user **seek back** to an offset within the **tracked offset range**. `LiKafkaConsumerImpl` will translate the user specified offset to ensure that no message will be "lost".
    * It is OK to seek to an ***"invisible"*** offset.
* `LiKafkaConsumerImpl` will throw an `OffsetNotTrackedException` if the user **seek back too far** (earlier than the offset of the first consumed message)
* **Seeking forward** always behave the same as vanilla open source consumer.
    * The consumer cannot track the messages it has not consumed yet.
* If a `LiKafkaConsumerImpl` has not consumed any messages from a partition, users can seek to any offset.
* Any successful seek operation on a partition will clear the tracked offsets for that partition in the `DeliveredMessageOffsetTracker`.
* `seekToBeginning()` and `seekToEnd()` behave the same as vanilla open source consumer

Due to the complexity of the `seek()` operation with large messages, users are encouraged to avoid seeking to an arbitrary offset if possible.

#### Log compacted topics ####
In general, li-apache-kafka-clients does not support large message handling for log compacted topics. In some specific cases, it might work by playing with the message keys, users may refer to [Handle Large Messages in Apache Kafka](http://www.slideshare.net/JiangjieQin/handle-large-messages-in-apache-kafka-58692297) if interested.

#### Record Headers ####
With li-apache-kafka-clients_1.0.17 and onwards, we have introduced client specific headers(with `_` as prefix) which are used to convey certain information about the messages. 
* `_so` => safe offset header; set only on the consumers when the safe offset is not the same as the high watermark. The contents are as follows: `safeOffset<8_byte_long[big-endian&signed]>`
* `_t` => create timestamp of the record. The contents are as follows: `timestamp<8_byte_long>[big-endian&signed]>` 
* `_lm` => large message header; The contents are as follows: `Type<1_byte> | UUID<8_byte_long[big-endian&signed]><8_byte_long[big-endian&signed]> | segmentNumber<4_byte_int[big-endian&signed]> | numberOfSegments<4_byte_int[big-endian&signed]>`.
