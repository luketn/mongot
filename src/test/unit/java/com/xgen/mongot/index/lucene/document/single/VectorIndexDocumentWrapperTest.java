package com.xgen.mongot.index.lucene.document.single;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import org.junit.Test;

public class VectorIndexDocumentWrapperTest {

  private static final VectorIndexCapabilities CURRENT_CAPABILITIES =
      new VectorIndexCapabilities(
          IndexFormatVersion.CURRENT, VectorIndexCapabilities.CURRENT_FEATURE_VERSION);

  private final IndexMetricsUpdater.IndexingMetricsUpdater metrics =
      new IndexMetricsUpdater.IndexingMetricsUpdater(
          SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH);

  @Test
  public void addIndexedVectorField_newField_incrementsCounter() {
    VectorIndexDocumentWrapper wrapper =
        VectorIndexDocumentWrapper.createRoot(new byte[8], CURRENT_CAPABILITIES, this.metrics);
    assertTrue(wrapper.canIndexVectorField("vector"));
    assertEquals(0.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);

    wrapper.addIndexedVectorField("vector");

    assertFalse(wrapper.canIndexVectorField("vector"));
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);
  }

  @Test
  public void addIndexedVectorField_duplicateField_isNoOp() {
    VectorIndexDocumentWrapper wrapper =
        VectorIndexDocumentWrapper.createRoot(new byte[8], CURRENT_CAPABILITIES, this.metrics);
    wrapper.addIndexedVectorField("vector");
    assertFalse(wrapper.canIndexVectorField("vector"));
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);

    wrapper.addIndexedVectorField("vector");

    assertFalse(wrapper.canIndexVectorField("vector"));
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);
  }

  @Test
  public void addIndexedVectorField_twoFields_isNoOp() {
    VectorIndexDocumentWrapper wrapper =
        VectorIndexDocumentWrapper.createRoot(new byte[8], CURRENT_CAPABILITIES, this.metrics);
    wrapper.addIndexedVectorField("vector");
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);

    wrapper.addIndexedVectorField("byteVector");

    assertFalse(wrapper.canIndexVectorField("byteVector"));
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);
  }
}
