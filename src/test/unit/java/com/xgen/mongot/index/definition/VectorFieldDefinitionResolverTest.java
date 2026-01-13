package com.xgen.mongot.index.definition;

import static com.xgen.mongot.index.definition.VectorIndexFieldDefinition.Type;
import static com.xgen.mongot.index.definition.VectorSimilarity.EUCLIDEAN;

import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.List;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class VectorFieldDefinitionResolverTest {

  @Test
  public void testIsUsed() {
    var definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("test")
            .database("db")
            .lastObservedCollectionName("col")
            .collectionUuid(UUID.randomUUID())
            .numPartitions(1)
            .setFields(
                List.of(
                    new VectorDataFieldDefinition(
                        FieldPath.parse("a.b.c"),
                        new VectorFieldSpecification(
                            100,
                            EUCLIDEAN,
                            VectorQuantization.NONE,
                            new VectorIndexingAlgorithm.HnswIndexingAlgorithm())),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("d")),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("e.f"))))
            .build();

    var resolver = new VectorFieldDefinitionResolver(definition, IndexFormatVersion.CURRENT);
    Assert.assertTrue(resolver.isUsed(FieldPath.parse("a")));
    Assert.assertTrue(resolver.isUsed(FieldPath.parse("a.b")));
    Assert.assertTrue(resolver.isUsed(FieldPath.parse("a.b.c")));
    Assert.assertTrue(resolver.isUsed(FieldPath.parse("d")));
    Assert.assertTrue(resolver.isUsed(FieldPath.parse("e")));
    Assert.assertTrue(resolver.isUsed(FieldPath.parse("e.f")));

    Assert.assertFalse(resolver.isUsed(FieldPath.parse("a.b.x")));
    Assert.assertFalse(resolver.isUsed(FieldPath.parse("d.x")));
    Assert.assertFalse(resolver.isUsed(FieldPath.parse("x")));
  }

  @Test
  public void testIsIndexed() {
    var definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("test")
            .database("db")
            .lastObservedCollectionName("col")
            .collectionUuid(UUID.randomUUID())
            .numPartitions(1)
            .setFields(
                List.of(
                    new VectorDataFieldDefinition(
                        FieldPath.parse("a.b.c"),
                        new VectorFieldSpecification(
                            100,
                            EUCLIDEAN,
                            VectorQuantization.NONE,
                            new VectorIndexingAlgorithm.HnswIndexingAlgorithm())),
                    new VectorTextFieldDefinition(FieldPath.parse("a.b.d")),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("d")),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("e.f"))))
            .build();

    var resolver = new VectorFieldDefinitionResolver(definition, IndexFormatVersion.CURRENT);

    // check exact path returns true
    Assert.assertTrue(resolver.isIndexed(FieldPath.parse("a.b.c"), Type.VECTOR));
    Assert.assertTrue(resolver.isIndexed(FieldPath.parse("a.b.d"), Type.TEXT));
    Assert.assertTrue(resolver.isIndexed(FieldPath.parse("d"), Type.FILTER));
    Assert.assertTrue(resolver.isIndexed(FieldPath.parse("e.f"), Type.FILTER));

    // check above and below the exact path returns false
    Assert.assertFalse(resolver.isIndexed(FieldPath.parse("a.b.c.d"), Type.VECTOR));
    Assert.assertFalse(resolver.isIndexed(FieldPath.parse("e"), Type.FILTER));
    Assert.assertFalse(resolver.isIndexed(FieldPath.parse("a.b.c"), Type.TEXT));

    // check exact path but type mismatch returns false
    Assert.assertFalse(resolver.isIndexed(FieldPath.parse("a.b.c"), Type.FILTER));
    Assert.assertFalse(resolver.isIndexed(FieldPath.parse("e.f"), Type.VECTOR));
    Assert.assertFalse(resolver.isIndexed(FieldPath.parse("a.b.d"), Type.VECTOR));
  }

  @Test
  public void testGetVectorFieldSpecification() {
    VectorFieldSpecification fieldSpecification =
        new VectorFieldSpecification(
            100,
            EUCLIDEAN,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm());

    var vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("test")
            .database("db")
            .lastObservedCollectionName("col")
            .collectionUuid(UUID.randomUUID())
            .numPartitions(1)
            .setFields(
                List.of(
                    new VectorDataFieldDefinition(
                        FieldPath.parse("foo.vector"), fieldSpecification),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("bar.filter"))))
            .build();

    var resolver =
        new VectorFieldDefinitionResolver(vectorIndexDefinition, IndexFormatVersion.CURRENT);
    var existingVector = resolver.getVectorFieldSpecification(FieldPath.parse("foo.vector"));
    Assert.assertEquals(fieldSpecification, existingVector.orElseThrow());

    var nonExistingVector = resolver.getVectorFieldSpecification(FieldPath.parse("foo.vector2"));
    Assert.assertTrue(nonExistingVector.isEmpty());

    var incorrectTypeDefinition =
        resolver.getVectorFieldSpecification(FieldPath.parse("bar.filter"));
    Assert.assertTrue(incorrectTypeDefinition.isEmpty());
  }
}
