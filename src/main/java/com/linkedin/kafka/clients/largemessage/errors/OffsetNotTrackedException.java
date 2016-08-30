/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.largemessage.errors;

/**
 * Thrown when an offset is not tracked by the consumer. LiKafkaConsumerImpl keeps track of all the messages it has
 * consumed to support large message aware seek(). This exception indicates that the users is trying to seek back
 * to an offset that is less than the earliest offset the consumer has ever consumed.
 */
public class OffsetNotTrackedException extends LargeMessageException {

  public OffsetNotTrackedException(String message) {
    super(message);
  }

}
