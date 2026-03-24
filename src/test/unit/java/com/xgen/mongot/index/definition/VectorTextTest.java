package com.xgen.mongot.index.definition;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.definition.VectorFieldSpecification.HnswOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class VectorTextTest {

  @Test
  public void testNonVector() {
    FieldPath path = FieldPath.parse("path");
    VectorDataFieldDefinition dataField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(path)
            .numDimensions(1)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    Assert.assertNotSame(dataField.getType(), VectorIndexFieldDefinition.Type.TEXT);
  }

  @Test
  public void testVectorTextDefinition() {
    FieldPath path = FieldPath.parse("path");
    VectorTextFieldDefinition textField = new VectorTextFieldDefinition(path);
    Assert.assertSame(VectorTextFieldDefinition.Type.TEXT, textField.getType());
    Assert.assertEquals(1024, textField.specification().numDimensions());
    Assert.assertEquals(VectorSimilarity.EUCLIDEAN, textField.specification().similarity());
    Assert.assertEquals(VectorQuantization.NONE, textField.specification().quantization());
  }

  @Test
  public void autoEmbedDefinition_defaultConfiguration() {
    FieldPath path = FieldPath.parse("path");
    VectorAutoEmbedFieldDefinition textField = new VectorAutoEmbedFieldDefinition("dummy", path);
    Assert.assertSame(VectorTextFieldDefinition.Type.AUTO_EMBED, textField.getType());
    Assert.assertEquals(1024, textField.specification().numDimensions());
    Assert.assertEquals(VectorSimilarity.DOT_PRODUCT, textField.specification().similarity());
    Assert.assertEquals(VectorQuantization.NONE, textField.specification().quantization());
  }

  // VectorAutoEmbedFieldDefinition.equals uses Objects.equals(this.specification,
  // that.specification).
  // VectorTextFieldSpecification extends VectorFieldSpecification, whose equals/hashCode includes
  // indexingAlgorithm (and thus hnswOptions). So equals/hashCode do include hnswOptions.

  @Test
  public void vectorAutoEmbedFieldDefinition_equals_samePathAndSpec_returnsTrue() {
    FieldPath path = FieldPath.parse("desc");
    VectorAutoEmbedFieldDefinition a =
        new VectorAutoEmbedFieldDefinition(
            "dummy",
            "text",
            path,
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.of(new HnswOptions(32, 200)));
    VectorAutoEmbedFieldDefinition b =
        new VectorAutoEmbedFieldDefinition(
            "dummy",
            "text",
            path,
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.of(new HnswOptions(32, 200)));
    assertTrue(a.equals(b));
    assertTrue(b.equals(a));
  }

  @Test
  public void vectorAutoEmbedFieldDefinition_equals_samePathAndSpecUsingDefault_returnsTrue() {
    FieldPath path = FieldPath.parse("desc");
    VectorAutoEmbedFieldDefinition a =
        new VectorAutoEmbedFieldDefinition(
            "dummy",
            "text",
            path,
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.empty());
    VectorAutoEmbedFieldDefinition b =
        new VectorAutoEmbedFieldDefinition(
            "dummy",
            "text",
            path,
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.empty());
    assertTrue(a.equals(b));
    assertTrue(b.equals(a));
  }

  @Test
  public void vectorAutoEmbedFieldDefinition_equals_samePathAndSpecOnlyOneDefault_returnsTrue() {
    FieldPath path = FieldPath.parse("desc");
    VectorAutoEmbedFieldDefinition a =
        new VectorAutoEmbedFieldDefinition(
            "dummy",
            "text",
            path,
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.of(new HnswOptions(16, 100)));
    VectorAutoEmbedFieldDefinition b =
        new VectorAutoEmbedFieldDefinition(
            "dummy",
            "text",
            path,
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.empty());
    assertTrue(a.equals(b));
    assertTrue(b.equals(a));
  }

  @Test
  public void vectorAutoEmbedFieldDefinition_equals_differentHnswOptions_returnsFalse() {
    FieldPath path = FieldPath.parse("desc");
    VectorAutoEmbedFieldDefinition defaultHnsw =
        new VectorAutoEmbedFieldDefinition(
            "dummy",
            "text",
            path,
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.of(new HnswOptions(64, 200)));
    VectorAutoEmbedFieldDefinition customHnsw =
        new VectorAutoEmbedFieldDefinition(
            "dummy",
            "text",
            path,
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.of(new HnswOptions(32, 200)));
    assertFalse(defaultHnsw.equals(customHnsw));
    assertFalse(customHnsw.equals(defaultHnsw));
  }

  @Test
  public void vectorAutoEmbedFieldDefinition_equals_differentPath_returnsFalse() {
    VectorAutoEmbedFieldDefinition a =
        new VectorAutoEmbedFieldDefinition(
            "dummy",
            "text",
            FieldPath.parse("desc"),
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.of(new HnswOptions(32, 200)));
    VectorAutoEmbedFieldDefinition b =
        new VectorAutoEmbedFieldDefinition(
            "dummy",
            "text",
            FieldPath.parse("title"),
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.of(new HnswOptions(32, 200)));
    assertFalse(a.equals(b));
    assertFalse(b.equals(a));
  }

  @Test
  public void vectorAutoEmbedFieldDefinition_equals_hashCodeConsistentWithEquals() {
    FieldPath path = FieldPath.parse("desc");
    VectorAutoEmbedFieldDefinition withCustomHnsw =
        new VectorAutoEmbedFieldDefinition(
            "dummy",
            "text",
            path,
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.of(new HnswOptions(16, 100)));
    VectorAutoEmbedFieldDefinition same =
        new VectorAutoEmbedFieldDefinition(
            "dummy",
            "text",
            path,
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.of(new HnswOptions(16, 100)));
    assertTrue(withCustomHnsw.equals(same));
  }
}
