package com.xgen.mongot.index.definition;

import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
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
    EmbeddingModelCatalog.enableMatView(false);
    VectorAutoEmbedFieldDefinition textField =
        new VectorAutoEmbedFieldDefinition("dummy", path);
    Assert.assertSame(VectorTextFieldDefinition.Type.AUTO_EMBED, textField.getType());
    Assert.assertEquals(1024, textField.specification().numDimensions());
    Assert.assertEquals(VectorSimilarity.DOT_PRODUCT, textField.specification().similarity());
    Assert.assertEquals(VectorQuantization.NONE, textField.specification().quantization());
  }
}
