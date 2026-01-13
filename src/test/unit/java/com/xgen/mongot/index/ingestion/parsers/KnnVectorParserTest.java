package com.xgen.mongot.index.ingestion.parsers;

import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.bson.Vector;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonBinaryReader;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.RawBsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.junit.Assert;
import org.junit.Test;

public class KnnVectorParserTest {

  @Test
  public void returnsValidVectorOnMixedInput() {
    var document =
        new RawBsonDocument(
            new BsonDocument(
                "field",
                new BsonArray(List.of(new BsonInt32(10), new BsonInt64(20), new BsonDouble(3.03)))),
            BsonUtils.BSON_DOCUMENT_CODEC);
    try (BsonBinaryReader bson =
        new BsonBinaryReader(new ByteBufferBsonInput(document.getByteBuffer()))) {
      bson.readStartDocument();
      Vector vector = KnnVectorParser.parse(bson).get();
      Assert.assertEquals(3, vector.numDimensions());
      Assert.assertEquals(Vector.VectorType.FLOAT, vector.getVectorType());
      Assert.assertArrayEquals(
          new float[] {10f, 20f, 3.03f}, vector.asFloatVector().getFloatVector(), 0f);
    }
  }

  @Test
  public void returnsLowerPrecisionVectorOnDoubleInput() {
    var document =
        new RawBsonDocument(
            new BsonDocument("field", new BsonArray(List.of(new BsonDouble(123.172123)))),
            BsonUtils.BSON_DOCUMENT_CODEC);
    try (BsonBinaryReader bson =
        new BsonBinaryReader(new ByteBufferBsonInput(document.getByteBuffer()))) {
      bson.readStartDocument();
      Vector vector = KnnVectorParser.parse(bson).get();
      Assert.assertEquals(1, vector.numDimensions());
      Assert.assertEquals(Vector.VectorType.FLOAT, vector.getVectorType());
      Assert.assertArrayEquals(
          new float[] {123.17213f}, vector.asFloatVector().getFloatVector(), 0f);
    }
  }

  @Test
  public void returnsApproximateVectorOnGreaterMagnitudeInput() {
    var document =
        new RawBsonDocument(
            new BsonDocument(
                "field",
                new BsonArray(
                    List.of(
                        new BsonDouble(Double.MAX_VALUE),
                        new BsonDouble(-Double.MAX_VALUE),
                        new BsonDouble(Double.MIN_VALUE),
                        new BsonDouble(-Double.MIN_VALUE)))),
            BsonUtils.BSON_DOCUMENT_CODEC);
    try (BsonBinaryReader bson =
        new BsonBinaryReader(new ByteBufferBsonInput(document.getByteBuffer()))) {
      bson.readStartDocument();
      Vector vector = KnnVectorParser.parse(bson).get();
      Assert.assertEquals(4, vector.numDimensions());
      Assert.assertEquals(Vector.VectorType.FLOAT, vector.getVectorType());
      Assert.assertArrayEquals(
          new float[] {Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 0f, 0f},
          vector.asFloatVector().getFloatVector(),
          0f);
    }
  }

  @Test
  public void returnsEmptyOptionalWhenArrayContainsNonNumberValues() {
    var document =
        new RawBsonDocument(
            new BsonDocument(
                "field",
                new BsonArray(
                    List.of(
                        new BsonDouble(1.23),
                        new BsonInt64(Long.MAX_VALUE),
                        new BsonBoolean(false)))),
            BsonUtils.BSON_DOCUMENT_CODEC);
    try (BsonBinaryReader bson =
        new BsonBinaryReader(new ByteBufferBsonInput(document.getByteBuffer()))) {
      bson.readStartDocument();
      Assert.assertTrue(KnnVectorParser.parse(bson).isEmpty());
    }
  }

  @Test
  public void returnsEmptyFloatArrayOnEmptyArrayInput() {
    var document =
        new RawBsonDocument(
            new BsonDocument("field", new BsonArray(List.of())), BsonUtils.BSON_DOCUMENT_CODEC);
    try (BsonBinaryReader bson =
        new BsonBinaryReader(new ByteBufferBsonInput(document.getByteBuffer()))) {
      bson.readStartDocument();
      Vector vector = KnnVectorParser.parse(bson).get();
      Assert.assertEquals(0, vector.numDimensions());
      Assert.assertArrayEquals(new float[0], vector.asFloatVector().getFloatVector(), 0.0f);
    }
  }
}
