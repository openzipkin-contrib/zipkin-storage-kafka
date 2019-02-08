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
package zipkin2.storage.kafka.internal.stores;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class IndexStateStore implements StateStore {
    private static final Logger LOG = LoggerFactory.getLogger(IndexStateStore.class);

    public static class Builder implements StoreBuilder<IndexStateStore> {
        final String name;

        boolean persistent;
        String indexDirectory;
        boolean loggingEnabled;

        Builder(String name) {
            this.name = name;
        }

        public Builder persistent(String indexDirectory) {
            this.persistent = true;
            this.indexDirectory = indexDirectory;
            return this;
        }

        public Builder inMemory() {
            this.persistent = false;
            return this;
        }

        public boolean isPersistent() {
            return persistent;
        }

        public String indexDirectory() {
            return indexDirectory;
        }

        @Override
        public Builder withCachingEnabled() {
            return null;
        }

        @Override
        public Builder withCachingDisabled() {
            throw new UnsupportedOperationException("caching not supported");
        }

        @Override
        public Builder withLoggingDisabled() {
            loggingEnabled = false;
            return this;
        }

        @Override
        public IndexStateStore build() {
            try {
                return new IndexStateStore(this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Map<String, String> logConfig() {
            return null;
        }

        @Override
        public boolean loggingEnabled() {
            return loggingEnabled;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Builder withLoggingEnabled(Map config) {
            loggingEnabled = true;
            return this;
        }
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    final String name;
    final boolean persistent;
    final IndexWriter indexWriter;

    IndexStateStore(Builder builder) throws IOException {
        final Directory directory;
        if (builder.isPersistent()) {
            LOG.info("Storing index on path={}", builder.indexDirectory);
            directory = new MMapDirectory(Paths.get(builder.indexDirectory));
        } else {
            directory = new ByteBuffersDirectory();
        }
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig indexWriterConfigs = new IndexWriterConfig(analyzer);
        indexWriter = new IndexWriter(directory, indexWriterConfigs);
//        indexWriter.commit();

        name = builder.name();
        persistent = builder.isPersistent();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        try {
            context.register(root, (key, value) -> {
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        try {
            indexWriter.close();
        } catch (IOException e) {
            LOG.error("Error closing index writer", e);
        }
    }

    @Override
    public boolean persistent() {
        return persistent;
    }

    @Override
    public boolean isOpen() {
        return indexWriter.isOpen();
    }

    public void put(List<Document> value) {
        try {
            for (Document doc : value) {
                indexWriter.addDocument(doc);
            }
            indexWriter.commit();
            LOG.info("{} indexed documents", value.size());
        } catch (IOException e) {
            LOG.error("Error indexing documents", e);
        }
    }

    public Directory directory() {
        return indexWriter.getDirectory();
    }
}
