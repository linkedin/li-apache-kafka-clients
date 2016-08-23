LiKafka-Clients
===================

### Introduction ###
LiKafka-Clients is a wrapper Kafka clients library built on top of Vanilla Apache Kafka clients.

Apache Kafka has now become a very popular messaging system and is well known for its low latency, high throughput and durable messaging. At LinkedIn, we have built an [ecosystem around Kafka](https://engineering.linkedin.com/blog/2016/04/kafka-ecosystem-at-linkedin) to power our infrastructure. In our ecosystem, LiKafka-Clients library is a fundamental component for many functionalities such as auditing, data format standardization, large message handling, and so on. LiKafka-Clients is designed to be fully compatible with Apache Kafka Vanilla clients. It is also highly customizable so the users can configure it according to their own environment.

### Features ###
LiKafka-Clients has the following features in addition to the vanilla Apache Kafka Java clients.

##### Large message support #####
Like many other messaging systems, Kafka has a maximum message size limit for a few reasons (e.g. memory management. Due to the message size limit, in some cases, user have to bare with small amount of message loss or involve additional storages to store the large objects. LiKafka-Clients addresses this problem with [a solution](http://www.slideshare.net/JiangjieQin/handle-large-messages-in-apache-kafka-58692297) that does not require an external storage dependency. For users who are storing offsets checkpoints in Kafka, LiKafka-Clients handles large messages almost transparently.

##### Auditing #####
LiKafka-Clients has integrated a pluggable auditing feature. Users may also have a custom auditing solution by implementing the defined Auditor interface. An AbstractAuditor and a CountingAuditStats class has also been provided to help the users implement the counting based auditing.

### When to use LiKafka-Clients ###
LiKafka-Clients is a wrapper library on top of vanilla Kafka Java clients. If one does not need the additional functions provided by LiKafka-Clients, the vanilla Kafka Java clients should be preferred.

### Build a jar and run all the unit tests ###
`./gradlew build`

### Future Work ###
##### Avro Serializer / Deserializer #####
We plan to add Avro serializer / deserializer which will also interact with schema registry.

##### Partition Expansion Safety #####
In Kafka, when partition expansion occurs, the partition associated with a certain key may change. Hence the consumer who was previously consuming a key may no longer consume that key any more. We plan to add the support to ensure that the same consumer will still be consuming the same key set before and after the partition number increases.

##### Sophisticated Auditing Implementation #####
In the first release of LiKafka-Clients, there is only a simple LoggingAuditor. In the future, we plan to add a more sophisticated auditing implementation we have at LinkedIn to LiKafka-Clients.