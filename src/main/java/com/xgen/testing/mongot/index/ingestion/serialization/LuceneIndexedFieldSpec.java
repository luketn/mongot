package com.xgen.testing.mongot.index.ingestion.serialization;

import com.xgen.mongot.index.lucene.extension.KnnFloatVectorField;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Set;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.bson.BsonDocument;

/**
 * Encodable document representing Lucene's {@link IndexableField}, used in the test framework for
 * serialization/deserialization purposes.
 */
public abstract class LuceneIndexedFieldSpec implements DocumentEncodable {

  private static final Set<DocValuesType> NUMERIC_DOC_VALUE_TYPES =
      Set.of(DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC);

  static class Fields {
    static final Field.Required<Type> TYPE =
        Field.builder("type").enumField(Type.class).asCamelCase().required();
  }

  enum Type {
    BINARY,
    GEO_SHAPE,
    KNN_VECTOR,
    LAT_LON,
    STORED_SOURCE,
    NUMERIC,
    STRING,
  }

  abstract BsonDocument fieldToBson();

  abstract Type getType();

  @Override
  public abstract boolean equals(Object o);

  @Override
  public abstract int hashCode();

  /**
   * Create a {@link LuceneIndexedFieldSpec} from an {@link IndexableField}, so that IndexableField
   * can be compared to what is expected.
   */
  public static LuceneIndexedFieldSpec fromIndexableField(IndexableField field) {
    IndexableFieldType type = field.fieldType();
    if (type.indexOptions() == IndexOptions.NONE
        && type.docValuesType() == DocValuesType.NONE
        && type.pointDimensionCount() == 2) {
      return LuceneIndexedLatLonPointField.fromField(field);
    }

    if (type.indexOptions() == IndexOptions.NONE
        && type.docValuesType() == DocValuesType.NONE
        && type.pointDimensionCount() == 7) {

      return LuceneIndexedGeoShapeField.fromField(field);
    }

    if (FieldName.StaticField.STORED_SOURCE.isTypeOf(field.name())) {
      return LuceneIndexedStoredSourceField.fromField(field);
    }

    // Numeric types (e.g. int64, int64_v2, double, dateFacet)
    if (type.pointDimensionCount() == 1 || NUMERIC_DOC_VALUE_TYPES.contains(type.docValuesType())) {
      return LuceneIndexedNumericField.fromField(field);
    }

    // String-like values (e.g. string, token, UUID, objectID, boolean)
    if (Objects.nonNull(field.stringValue())) {
      return LuceneIndexedStringField.fromField(field);
    } else if (Objects.nonNull(field.binaryValue())) {
      return LuceneIndexedBinaryField.fromField(field);
    }

    // Vector fields: Must downcast to access vector data
    if (type.vectorDimension() > 0) {
      if (field instanceof KnnFloatVectorField floatVectorField) {
        return LuceneIndexedKnnVectorField.fromField(floatVectorField);
      } else if (field instanceof KnnByteVectorField byteVectorField) {
        return LuceneIndexedKnnVectorField.fromField(byteVectorField);
      }
    }

    // throw if there is a field type we haven't explicitly handled
    throw new AssertionError("unknown indexable field: " + type);
  }

  static LuceneIndexedFieldSpec fromBson(DocumentParser parser) throws BsonParseException {
    var type = parser.getField(Fields.TYPE).unwrap();
    return switch (type) {
      case BINARY -> LuceneIndexedBinaryField.fromBson(parser);
      case GEO_SHAPE -> LuceneIndexedGeoShapeField.fromBson(parser);
      case KNN_VECTOR -> LuceneIndexedKnnVectorField.fromBson(parser);
      case LAT_LON -> LuceneIndexedLatLonPointField.fromBson(parser);
      case STORED_SOURCE -> LuceneIndexedStoredSourceField.fromBson(parser);
      case NUMERIC -> LuceneIndexedNumericField.fromBson(parser);
      case STRING -> LuceneIndexedStringField.fromBson(parser);
    };
  }

  @Override
  public BsonDocument toBson() {
    BsonDocument doc = BsonDocumentBuilder.builder().field(Fields.TYPE, getType()).build();
    doc.putAll(fieldToBson());
    return doc;
  }
}
