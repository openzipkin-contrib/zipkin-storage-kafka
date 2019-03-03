/*
 * Copyright 2019 jeqo
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
package zipkin2.storage.kafka.streams.stores;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

import java.util.List;
import java.util.Optional;

public class IndexStoreType implements QueryableStoreType<IndexStateStore> {
  @Override
  public boolean accepts(StateStore stateStore) {
    return stateStore.isOpen();
  }

  @Override
  public IndexStateStore create(StateStoreProvider storeProvider, String storeName) {
    List<IndexStateStore> stores = storeProvider.stores(storeName, this);
    final Optional<IndexStateStore> value = stores.stream().findFirst();
    if (value.isPresent()) {
      return value.get();
    } else {
      throw new Error("Non index storage found");
    }
  }
}
