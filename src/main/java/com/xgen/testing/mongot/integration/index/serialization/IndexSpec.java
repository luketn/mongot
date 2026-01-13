package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;

public abstract class IndexSpec implements DocumentEncodable {

  public enum Type {
    SEARCH,
    VECTOR_SEARCH
  }

  static class Fields {
    static final Field.WithDefault<IndexDefinition.Type> TYPE =
        Field.builder("type")
            .enumField(IndexDefinition.Type.class)
            .asCamelCase()
            .optional()
            .withDefault(IndexDefinition.Type.SEARCH);
  }

  public abstract Type getType();

  /**
   * The index format versions to run a test case against.
   *
   * @return list of index format versions to test against
   */
  public abstract List<IndexFormatVersion> getIndexFormatVersions();

  /**
   * The index feature versions to run a test case against. The list is generated using the {@link
   * SearchIndexFeatureVersionSpec} or the {@link VectorIndexFeatureVersionSpec} depending on the
   * tes case.
   *
   * @return list of index feature version to run the test case against
   */
  public abstract List<Integer> getIndexFeatureVersions();

  /**
   * Returns the number of index partitions configured for the index. The range of possible values
   * are the default value (1) and any powers of 2.
   */
  public abstract int getNumPartitions();

  public SearchIndexSpec asSearch() {
    Check.expectedType(Type.SEARCH, this.getType());
    return (SearchIndexSpec) this;
  }

  public VectorIndexSpec asVector() {
    Check.expectedType(Type.VECTOR_SEARCH, this.getType());
    return (VectorIndexSpec) this;
  }

  public static IndexSpec fromBson(DocumentParser parser) throws BsonParseException {
    var type = parser.getField(Fields.TYPE).unwrap();
    if (type == IndexDefinition.Type.VECTOR_SEARCH) {
      return VectorIndexSpec.fromBson(parser);
    }
    return SearchIndexSpec.fromBson(parser);
  }
}
