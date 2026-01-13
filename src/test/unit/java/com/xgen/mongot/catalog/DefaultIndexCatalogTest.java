package com.xgen.mongot.catalog;

import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_COLLECTION_UUID;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_DATABASE_NAME;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_ID;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_NAME;

import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.version.Generation;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class DefaultIndexCatalogTest {

  @Test
  public void testGetIndexCreatedOnCollection() {
    DefaultIndexCatalog catalog = new DefaultIndexCatalog();
    VectorIndexDefinitionGeneration generation =
        new VectorIndexDefinitionGeneration(VectorIndex.MOCK_VECTOR_DEFINITION, Generation.FIRST);
    IndexGeneration index = new IndexGeneration(VectorIndex.mockIndex(generation), generation);

    catalog.addIndex(index);

    Optional<IndexGeneration> foundIndex =
        catalog.getIndex(
            index.getDefinition().getDatabase(),
            index.getDefinition().getCollectionUuid(),
            Optional.empty(),
            index.getDefinition().getName());
    Assert.assertTrue(foundIndex.isPresent());
    Assert.assertEquals(index, foundIndex.get());
  }

  @Test
  public void testGetIndexCreatedOnView() {
    DefaultIndexCatalog catalog = new DefaultIndexCatalog();
    VectorIndexDefinition definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(MOCK_INDEX_ID)
            .name(MOCK_INDEX_NAME)
            .database(MOCK_INDEX_DATABASE_NAME)
            .lastObservedCollectionName(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
            .collectionUuid(MOCK_INDEX_COLLECTION_UUID)
            .view(ViewDefinition.existing("test", List.of()))
            .withFilterPath("filter.path")
            .withCosineVectorField("vector.path", 3)
            .build();

    VectorIndexDefinitionGeneration generation =
        new VectorIndexDefinitionGeneration(definition, Generation.FIRST);
    IndexGeneration index = new IndexGeneration(VectorIndex.mockIndex(generation), generation);

    catalog.addIndex(index);

    Optional<IndexGeneration> foundIndex =
        catalog.getIndex(
            index.getDefinition().getDatabase(),
            index.getDefinition().getCollectionUuid(),
            Optional.of("test"),
            index.getDefinition().getName());
    Assert.assertTrue(foundIndex.isPresent());
    Assert.assertEquals(index, foundIndex.get());
  }

  @Test
  public void testGetIndexById() {
    DefaultIndexCatalog catalog = new DefaultIndexCatalog();
    VectorIndexDefinitionGeneration generation =
        new VectorIndexDefinitionGeneration(VectorIndex.MOCK_VECTOR_DEFINITION, Generation.FIRST);
    IndexGeneration index = new IndexGeneration(VectorIndex.mockIndex(generation), generation);

    catalog.addIndex(index);

    Optional<IndexGeneration> foundIndex = catalog.getIndexById(index.getDefinition().getIndexId());
    Assert.assertTrue(foundIndex.isPresent());
    Assert.assertEquals(index, foundIndex.get());
  }

  @Test
  public void testRemoveIndex() {
    DefaultIndexCatalog catalog = new DefaultIndexCatalog();
    VectorIndexDefinitionGeneration generation =
        new VectorIndexDefinitionGeneration(VectorIndex.MOCK_VECTOR_DEFINITION, Generation.FIRST);
    IndexGeneration index = new IndexGeneration(VectorIndex.mockIndex(generation), generation);

    catalog.addIndex(index);

    Optional<IndexGeneration> removedIndex =
        catalog.removeIndex(index.getDefinition().getIndexId());
    Assert.assertTrue(removedIndex.isPresent());
    Assert.assertEquals(index, removedIndex.get());
    Assert.assertEquals(0, catalog.getSize());
  }
}
