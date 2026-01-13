package com.xgen.testing.mongot.index.ingestion.serialization;

import static java.util.stream.Collectors.toList;

import com.google.common.base.Objects;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Floats;
import com.xgen.mongot.index.lucene.extension.KnnFloatVectorField;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonNumber;

public class LuceneIndexedKnnVectorField extends LuceneIndexedFieldSpec {

  static class Fields {
    static final Field.Required<List<BsonNumber>> VALUE =
        Field.builder("value").bsonNumberField().asList().required();
    static final Field.Required<Integer> NUM_DIMENSION =
        Field.builder("numDimension").intField().required();
    static final Field.Required<VectorEncoding> ENCODING =
        Field.builder("encoding").enumField(VectorEncoding.class).asCamelCase().required();
    static final Field.Required<VectorSimilarityFunction> SIMILARITY =
        Field.builder("similarity")
            .enumField(VectorSimilarityFunction.class)
            .asCamelCase()
            .required();
  }

  private final List<BsonNumber> value;
  private final int numDimension;
  private final VectorEncoding encoding;
  private final VectorSimilarityFunction similarity;

  public LuceneIndexedKnnVectorField(
      List<BsonNumber> value,
      int numDimension,
      VectorEncoding encoding,
      VectorSimilarityFunction similarity) {
    this.value = value;
    this.numDimension = numDimension;
    this.encoding = encoding;
    this.similarity = similarity;
  }

  static LuceneIndexedKnnVectorField fromBson(DocumentParser parser) throws BsonParseException {
    return new LuceneIndexedKnnVectorField(
        parser.getField(Fields.VALUE).unwrap(),
        parser.getField(Fields.NUM_DIMENSION).unwrap(),
        parser.getField(Fields.ENCODING).unwrap(),
        parser.getField(Fields.SIMILARITY).unwrap());
  }

  static LuceneIndexedKnnVectorField fromField(KnnFloatVectorField field) {
    List<BsonNumber> vector =
        Floats.asList(field.vectorValue()).stream().map(BsonDouble::new).collect(toList());

    IndexableFieldType type = field.fieldType();
    return new LuceneIndexedKnnVectorField(
        vector, type.vectorDimension(), type.vectorEncoding(), type.vectorSimilarityFunction());
  }

  static LuceneIndexedKnnVectorField fromField(KnnByteVectorField field) {
    List<BsonNumber> vector =
        Bytes.asList(field.vectorValue()).stream().map(BsonInt32::new).collect(toList());
    IndexableFieldType type = field.fieldType();
    return new LuceneIndexedKnnVectorField(
        vector, type.vectorDimension(), type.vectorEncoding(), type.vectorSimilarityFunction());
  }

  @Override
  BsonDocument fieldToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.VALUE, this.value)
        .field(Fields.NUM_DIMENSION, this.numDimension)
        .field(Fields.ENCODING, this.encoding)
        .field(Fields.SIMILARITY, this.similarity)
        .build();
  }

  @Override
  Type getType() {
    return Type.KNN_VECTOR;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LuceneIndexedKnnVectorField that)) {
      return false;
    }
    return this.numDimension == that.numDimension
        && Objects.equal(this.value, that.value)
        && this.encoding == that.encoding
        && this.similarity == that.similarity;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.value, this.numDimension, this.encoding, this.similarity);
  }
}
