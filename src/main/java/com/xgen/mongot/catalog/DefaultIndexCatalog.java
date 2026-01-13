package com.xgen.mongot.catalog;

import com.google.common.base.Objects;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.bson.types.ObjectId;

public class DefaultIndexCatalog implements IndexCatalog {

  /**
   * A fair lock to ensure writes eventually succeed.
   *
   * <p>The actions under the lock are Map manipulations, so the locks shouldn't be held for very
   * long.
   */
  private final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);

  private final Lock readLock = this.reentrantReadWriteLock.readLock();
  private final Lock writeLock = this.reentrantReadWriteLock.writeLock();

  private final Map<IndexProperties, IndexGeneration> indexes;
  private final Map<ObjectId, IndexGeneration> indexesById;

  public DefaultIndexCatalog() {
    this.indexes = new HashMap<>();
    this.indexesById = new HashMap<>();
  }

  @Override
  public void addIndex(IndexGeneration indexGeneration) {
    this.writeLock.lock();
    try {
      this.indexes.put(new IndexProperties(indexGeneration.getDefinition()), indexGeneration);
      this.indexesById.put(indexGeneration.getDefinition().getIndexId(), indexGeneration);
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public Optional<IndexGeneration> getIndex(
      String databaseName, UUID collectionUuid, Optional<String> viewName, String indexName) {
    this.readLock.lock();
    try {
      IndexProperties properties = new IndexProperties(indexName, databaseName, collectionUuid);
      Optional<IndexGeneration> index = Optional.ofNullable(this.indexes.get(properties));

      if (index.isPresent() && viewName.isPresent()) {
        Optional<String> viewNameFromIndex =
            index.get().getDefinition().getView().map(ViewDefinition::getName);
        if (!viewNameFromIndex.equals(viewName)) {
          // we found index by name and collection uuid, but the view name does not match,
          // meaning that user made a typo or tried to query an index that does not exist
          return Optional.empty();
        }
      }

      return index;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Optional<IndexGeneration> getIndexById(ObjectId indexId) {
    this.readLock.lock();
    try {
      return Optional.ofNullable(this.indexesById.get(indexId));
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Collection<IndexGeneration> getIndexes() {
    this.readLock.lock();
    try {
      return new ArrayList<>(this.indexesById.values());
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public int getSize() {
    this.readLock.lock();
    try {
      return this.indexesById.size();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Optional<IndexGeneration> removeIndex(ObjectId indexId) {
    this.writeLock.lock();
    try {
      IndexGeneration indexGeneration = this.indexesById.remove(indexId);
      if (indexGeneration == null) {
        return Optional.empty();
      }

      IndexDefinition definition = indexGeneration.getDefinition();
      IndexProperties properties = new IndexProperties(definition);
      this.indexes.remove(properties);
      return Optional.of(indexGeneration);
    } finally {
      this.writeLock.unlock();
    }
  }

  private static class IndexProperties {

    private final String indexName;
    private final String databaseName;
    private final UUID collectionUuid;

    public IndexProperties(String indexName, String databaseName, UUID collectionUuid) {
      this.indexName = indexName;
      this.databaseName = databaseName;
      this.collectionUuid = collectionUuid;
    }

    public IndexProperties(IndexDefinition definition) {
      this.indexName = definition.getName();
      this.databaseName = definition.getDatabase();
      this.collectionUuid = definition.getCollectionUuid();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IndexProperties)) {
        return false;
      }
      IndexProperties that = (IndexProperties) o;
      return Objects.equal(this.indexName, that.indexName)
          && Objects.equal(this.databaseName, that.databaseName)
          && Objects.equal(this.collectionUuid, that.collectionUuid);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(this.indexName, this.databaseName, this.collectionUuid);
    }
  }
}
