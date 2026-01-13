package com.xgen.mongot.index.lucene.util;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexVectorFieldDefinition;
import com.xgen.mongot.util.FieldPath;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utility class for extracting vector field specifications for LuceneCodec construction. */
public final class LuceneCodecUtils {

  private LuceneCodecUtils() {
    // Do not instantiate
  }

  /**
   * Extracts vector field specifications from a list of vector index field definitions.
   *
   * @param fields list of vector index field definitions
   * @return map from field path to vector field specification
   */
  public static Map<FieldPath, VectorFieldSpecification> extractVectorFieldsFromVectorIndex(
      List<VectorIndexFieldDefinition> fields) {
    List<VectorIndexVectorFieldDefinition> vectorFields =
        fields.stream()
            .filter(VectorIndexFieldDefinition::isVectorField)
            .map(VectorIndexFieldDefinition::asVectorField)
            .toList();

    Map<FieldPath, VectorFieldSpecification> pathToField = new HashMap<>();
    for (VectorIndexVectorFieldDefinition vectorField : vectorFields) {
      pathToField.put(vectorField.getPath(), vectorField.specification());
    }
    return pathToField;
  }

  /**
   * Extracts vector field specifications from search index field mappings.
   *
   * @param fields map of field names to field definitions from search index mappings
   * @return map from field path to vector field specification
   */
  public static Map<FieldPath, VectorFieldSpecification> extractVectorFieldsFromSearchMappings(
      ImmutableMap<String, FieldDefinition> fields) {
    Map<FieldPath, VectorFieldSpecification> pathToField = new HashMap<>();
    for (String fieldName : fields.keySet()) {
      FieldDefinition fieldDefinition = fields.get(fieldName);
      if (fieldDefinition.vectorFieldSpecification().isEmpty()) {
        continue;
      }
      VectorFieldSpecification fieldSpecification =
          fieldDefinition.vectorFieldSpecification().get();
      pathToField.put(FieldPath.parse(fieldName), fieldSpecification);
    }
    return pathToField;
  }
}
