/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.utils;

import java.util.UUID;

/**
 * Creates UUIDs.
 */
public interface UUIDFactory {

  UUID create();

}
