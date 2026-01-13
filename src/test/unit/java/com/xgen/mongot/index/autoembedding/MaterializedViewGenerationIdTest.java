package com.xgen.mongot.index.autoembedding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.common.collect.Sets;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;
import org.junit.Test;

public class MaterializedViewGenerationIdTest {

  @Test
  public void testConstructor() {
    ObjectId objectId = new ObjectId();
    GenerationId value = GenerationIdBuilder.create(objectId, 1, 2, 1);
    assertEquals(objectId, value.indexId);
    MaterializedViewGenerationId materializedViewGenerationId =
        new MaterializedViewGenerationId(
            value.indexId, new MaterializedViewGeneration(value.generation));
    assertNotEquals(materializedViewGenerationId, value);
    assertEquals(
        materializedViewGenerationId.uniqueString(), "matview-%s-u1".formatted(value.indexId));
  }

  public static List<CheckedSupplier<MaterializedViewGenerationId, RuntimeException>>
      materializedViewGenerationIdSuppliers(
          Set<ObjectId> objectIds,
          Set<Integer> userVersions,
          Set<Integer> indexFormatVersions,
          Set<Integer> attempts) {

    // Take the Cartesian product of all the userVersion, indexFormatVersions, and attempts.
    List<Generation> sourceGenerations =
        Sets.cartesianProduct(userVersions, indexFormatVersions, attempts).stream()
            .map(
                params ->
                    new Generation(
                        new UserIndexVersion(params.get(0)),
                        IndexFormatVersion.create(params.get(1)),
                        params.get(2)))
            .toList();

    // Now we Cartesian product the Generations with the ObjectIds to get GenerationId suppliers.
    return objectIds.stream()
        .flatMap(
            objectId ->
                sourceGenerations.stream()
                    .<CheckedSupplier<MaterializedViewGenerationId, RuntimeException>>map(
                        generation ->
                            () ->
                                new MaterializedViewGeneration(generation).generationId(objectId)))
        .collect(Collectors.toList());
  }

  /** Ensure that distinct GenerationIds have distinct uniqueStrings. */
  @Test
  public void testUniqueStringDistinctness() {

    Set<ObjectId> objectIds = Set.of(new ObjectId(), new ObjectId());
    Set<Integer> userVersions = Set.of(1, 10);
    Set<Integer> indexFormatVersions = Set.of(2, 10);
    Set<Integer> attempts = Set.of(1, 2);

    List<String> uniqueStrings =
        materializedViewGenerationIdSuppliers(
                objectIds, userVersions, indexFormatVersions, attempts)
            .stream()
            .map(CheckedSupplier::get)
            .map(MaterializedViewGenerationId::uniqueString)
            .toList();
    Set<String> uniqueStringSet = new HashSet<>(uniqueStrings);

    assertEquals(
        "Materialized View GenerationIDs are decided by indexIDs and user versions only.",
        uniqueStringSet.size(),
        objectIds.size() * userVersions.size());
  }
}
