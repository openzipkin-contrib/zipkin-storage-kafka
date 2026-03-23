/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.module.storage.kafka;

import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * opens package access for testing
 */
public final class Access {
  public static void registerKafka(AnnotationConfigApplicationContext context) {
    context.register(PropertyPlaceholderAutoConfiguration.class, ZipkinKafkaStorageModule.class);
  }
}
