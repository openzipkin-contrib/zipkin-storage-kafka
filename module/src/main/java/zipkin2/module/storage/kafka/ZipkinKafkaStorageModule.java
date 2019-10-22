/*
 * Copyright 2019 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.module.storage.kafka;

import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.spring.ArmeriaServerConfigurator;
import java.util.List;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.kafka.KafkaStorage;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ZipkinKafkaStorageProperties.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "kafka")
@ConditionalOnMissingBean(StorageComponent.class)
class ZipkinKafkaStorageModule {
  static final String QUALIFIER = "zipkinKafkaStorage";

  @ConditionalOnMissingBean @Qualifier(QUALIFIER) @Bean StorageComponent storage(
      @Value("${zipkin.storage.search-enabled:true}") boolean searchEnabled,
      @Value("${zipkin.storage.autocomplete-keys:}") List<String> autocompleteKeys,
      ZipkinKafkaStorageProperties properties) {
    return properties.toBuilder()
        .searchEnabled(searchEnabled)
        .autocompleteKeys(autocompleteKeys)
        .build();
  }

  @Qualifier(QUALIFIER) @Bean public ArmeriaServerConfigurator storageHttpService(
      @Qualifier(QUALIFIER) StorageComponent storage) {
    return sb -> sb.annotatedService("/storage/kafka", ((KafkaStorage) storage).httpService());
  }
}
