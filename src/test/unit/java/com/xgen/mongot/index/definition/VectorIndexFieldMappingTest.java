package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class VectorIndexFieldMappingTest {

  @Test
  public void testSimple() {
    FieldPath path = FieldPath.parse("path");
    VectorDataFieldDefinition dataField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(path)
            .numDimensions(1)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    List<VectorIndexFieldDefinition> fields = List.of(dataField);
    VectorIndexFieldMapping mapping = VectorIndexFieldMapping.create(fields);
    Assert.assertEquals(mapping.getFieldDefinition(path), Optional.of(dataField));
    Assert.assertFalse(mapping.subDocumentExists(path));
  }

  @Test
  public void testSingleFilter() {
    FieldPath dataPath = FieldPath.parse("data");
    VectorDataFieldDefinition dataField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(dataPath)
            .numDimensions(1)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    FieldPath filterPath = FieldPath.parse("filter");
    VectorIndexFilterFieldDefinition filterField =
        VectorIndexFilterFieldDefinition.create(filterPath);
    List<VectorIndexFieldDefinition> fields = List.of(dataField, filterField);
    VectorIndexFieldMapping mapping = VectorIndexFieldMapping.create(fields);
    Assert.assertEquals(mapping.getFieldDefinition(dataPath), Optional.of(dataField));
    Assert.assertEquals(mapping.getFieldDefinition(filterPath), Optional.of(filterField));
    Assert.assertFalse(mapping.subDocumentExists(filterPath));
  }

  @Test
  public void testMultipleFiltersSameParentPath() {
    FieldPath dataPath = FieldPath.parse("data");
    VectorDataFieldDefinition dataField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(dataPath)
            .numDimensions(1)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    FieldPath filterParent = FieldPath.parse("filter");
    FieldPath filterPath1 = FieldPath.parse("filter.date");
    VectorIndexFilterFieldDefinition filterField1 =
        VectorIndexFilterFieldDefinition.create(filterPath1);
    FieldPath filterPath2 = FieldPath.parse("filter.number");
    VectorIndexFilterFieldDefinition filterField2 =
        VectorIndexFilterFieldDefinition.create(filterPath2);
    List<VectorIndexFieldDefinition> fields = List.of(dataField, filterField1, filterField2);
    VectorIndexFieldMapping mapping = VectorIndexFieldMapping.create(fields);
    Assert.assertEquals(mapping.getFieldDefinition(dataPath), Optional.of(dataField));
    Assert.assertEquals(mapping.getFieldDefinition(filterPath1), Optional.of(filterField1));
    Assert.assertEquals(mapping.getFieldDefinition(filterPath2), Optional.of(filterField2));
    Assert.assertTrue(mapping.subDocumentExists(filterParent));
    Assert.assertFalse(mapping.subDocumentExists(filterPath1));
    Assert.assertFalse(mapping.subDocumentExists(filterPath2));
  }

  @Test
  public void testTwoFieldsSamePath() {
    FieldPath path = FieldPath.parse("path");
    VectorDataFieldDefinition dataField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(path)
            .numDimensions(1)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    VectorIndexFilterFieldDefinition filterField = VectorIndexFilterFieldDefinition.create(path);
    List<VectorIndexFieldDefinition> fields = List.of(dataField, filterField);
    Assert.assertThrows(
        IllegalArgumentException.class, () -> VectorIndexFieldMapping.create(fields));
  }

  @Test
  public void testPathIsBothDocumentAndField() {
    FieldPath dataPath = FieldPath.parse("data");
    VectorDataFieldDefinition dataField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(dataPath)
            .numDimensions(1)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    FieldPath filterParentPath = FieldPath.parse("filter");
    FieldPath filterPath = FieldPath.parse("filter.date");
    VectorIndexFilterFieldDefinition filterParentField =
        VectorIndexFilterFieldDefinition.create(filterParentPath);
    VectorIndexFilterFieldDefinition filterField =
        VectorIndexFilterFieldDefinition.create(filterPath);
    List<VectorIndexFieldDefinition> fields = List.of(dataField, filterParentField, filterField);
    VectorIndexFieldMapping mapping = VectorIndexFieldMapping.create(fields);
    Assert.assertEquals(mapping.getFieldDefinition(dataPath), Optional.of(dataField));
    Assert.assertEquals(
        mapping.getFieldDefinition(filterParentPath), Optional.of(filterParentField));
    Assert.assertEquals(mapping.getFieldDefinition(filterPath), Optional.of(filterField));
    Assert.assertTrue(mapping.subDocumentExists(filterParentPath));
    Assert.assertFalse(mapping.subDocumentExists(filterPath));
  }
}
