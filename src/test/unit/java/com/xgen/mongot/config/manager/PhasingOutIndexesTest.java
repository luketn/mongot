package com.xgen.mongot.config.manager;

import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;

import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class PhasingOutIndexesTest {
  @Test
  public void testEmpty() {
    var col = new PhasingOutIndexes();
    Assert.assertEquals(Collections.emptyList(), col.getIndexes());
  }

  @Test
  public void testDuplicateGenerationIdThrows() {
    var col = new PhasingOutIndexes();
    var index = mockIndexGeneration();
    col.addIndex(index);
    Assert.assertThrows(RuntimeException.class, () -> col.addIndex(index));
  }

  @Test
  public void testRemoveIndex() {
    var col = new PhasingOutIndexes();
    var index = mockIndexGeneration();
    Assert.assertFalse(col.removeIndex(index));

    col.addIndex(index);
    Assert.assertEquals(List.of(index), col.getIndexes());

    Assert.assertTrue(col.removeIndex(index));
    Assert.assertEquals(Collections.emptyList(), col.getIndexes());
  }

  @Test
  public void testGetByIndexId() {
    var col = new PhasingOutIndexes();
    var index = mockIndexGeneration(MOCK_INDEX_GENERATION_ID);
    var index2 = mockIndexGeneration(GenerationIdBuilder.incrementUser(MOCK_INDEX_GENERATION_ID));

    ObjectId id = index.getDefinition().getIndexId();
    Assert.assertTrue(col.getIndexesById(id).isEmpty());

    col.addIndex(index);

    Assert.assertEquals(List.of(index), col.getIndexesById(id));
    Assert.assertEquals(List.of(), col.getIndexesById(new ObjectId()));

    col.addIndex(index2);
    // order not important:
    Assert.assertEquals(Set.of(index, index2), Set.copyOf(col.getIndexesById(id)));

    // deletes reflect in getIndexes()
    col.removeIndex(index);
    col.removeIndex(index2);
    Assert.assertEquals(List.of(), col.getIndexesById(id));
    Assert.assertEquals(List.of(), col.getIndexesById(new ObjectId()));
  }
}
