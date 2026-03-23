/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.module.storage.kafka;

import com.linecorp.armeria.spring.ArmeriaServerConfigurator;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.kafka.KafkaStorage;

import static zipkin2.storage.kafka.KafkaStorage.HTTP_PATH_PREFIX;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ZipkinKafkaStorageProperties.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "kafka")
@ConditionalOnMissingBean(StorageComponent.class)
class ZipkinKafkaStorageModule {

  @ConditionalOnMissingBean @Bean StorageComponent storage(
    @Value("${zipkin.storage.search-enabled:true}") boolean searchEnabled,
    @Value("${zipkin.storage.autocomplete-keys:}") List<String> autocompleteKeys,
    @Value("${server.port:9411}") int port,
    ZipkinKafkaStorageProperties properties) {
    return properties.toBuilder()
      .searchEnabled(searchEnabled)
      .autocompleteKeys(autocompleteKeys)
      .serverPort(port)
      .build();
  }

  @Bean public ArmeriaServerConfigurator storageHttpService(StorageComponent storage) {
    return sb -> sb.annotatedService(HTTP_PATH_PREFIX, ((KafkaStorage) storage).httpService());
  }
}
