/*
 * Copyright 2019 [name of copyright owner]
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
package zipkin2.storage.kafka.internal;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

import java.util.List;
import java.util.Optional;

public class LuceneStoreType implements QueryableStoreType<LuceneStateStore> {
    @Override
    public boolean accepts(StateStore stateStore) {
        return stateStore.isOpen();
    }

    @Override
    public LuceneStateStore create(StateStoreProvider storeProvider, String storeName) {
        List<LuceneStateStore> stores = storeProvider.stores(storeName, this);
        final Optional<LuceneStateStore> value = stores.stream().findFirst();
        return value.get();
    }
}
