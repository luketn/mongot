package com.xgen.mongot.util.bson;

import static com.xgen.mongot.util.bson.ProtoConverter.DECIMAL128_BYTE_LENGTH;
import static com.xgen.mongot.util.bson.ProtoConverter.OBJECT_ID_BYTE_LENGTH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import com.xgen.mongot.proto.bson.ProtoDocument;
import com.xgen.mongot.proto.bson.ProtoValue;
import com.xgen.mongot.util.BsonUtils;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class ProtoConverterTest {

  @DataPoints
  public static BsonValue[] bsonValues =
      new BsonValue[] {
        BsonUtils.MIN_KEY,
        BsonNull.VALUE,
        new BsonUndefined(),
        new BsonInt32(1),
        new BsonInt64(2),
        new BsonDouble(3.14),
        new BsonDecimal128(Decimal128.fromIEEE754BIDEncoding(1001, 2002)),
        new BsonString("5"),
        new BsonDateTime(6),
        new BsonTimestamp(7),
        new BsonBinary(UUID.randomUUID()),
        new BsonBinary(BsonBinarySubType.BINARY, RandomUtils.nextBytes(16)),
        new BsonBoolean(true),
        new BsonArray(),
        new BsonArray(
            List.of(
                BsonNull.VALUE, new BsonDocument("1", new BsonString("nested")), new BsonInt64(1))),
        new BsonRegularExpression(".*", "i"),
        new BsonJavaScript("function test() {}"),
        new BsonObjectId(new ObjectId()),
        new BsonDocument(),
        new BsonDocument("nested", new BsonDocument("key", new BsonBinary(UUID.randomUUID()))),
        new BsonDbPointer("ns", new ObjectId()),
        new BsonSymbol("symbol"),
        new BsonJavaScriptWithScope("code", new BsonDocument("test", new BsonBoolean(true))),
        BsonUtils.MAX_KEY,
      };

  @Theory
  public void toProto(BsonValue original) throws TypeConversionException {
    ProtoValue proto = ProtoConverter.convert(original);

    BsonValue recovered = ProtoConverter.convert(proto);

    assertEquals(original, recovered);
  }

  @Test
  public void objectIdTooLong() {
    ProtoValue value =
        ProtoValue.newBuilder()
            .setObjectId(ByteString.copyFrom(new byte[OBJECT_ID_BYTE_LENGTH + 4]))
            .build();

    assertThrows(TypeConversionException.class, () -> ProtoConverter.convert(value));
  }

  @Test
  public void objectIdTooShort() {
    ProtoValue value =
        ProtoValue.newBuilder()
            .setObjectId(ByteString.copyFrom(new byte[OBJECT_ID_BYTE_LENGTH - 4]))
            .build();

    assertThrows(TypeConversionException.class, () -> ProtoConverter.convert(value));
  }

  @Test
  public void decimal128TooLong() {
    ProtoValue value =
        ProtoValue.newBuilder()
            .setDecimal128(ByteString.copyFrom(new byte[DECIMAL128_BYTE_LENGTH + 4]))
            .build();

    assertThrows(TypeConversionException.class, () -> ProtoConverter.convert(value));
  }

  @Test
  public void decimal128TooShort() {
    ProtoValue value =
        ProtoValue.newBuilder()
            .setDecimal128(ByteString.copyFrom(new byte[DECIMAL128_BYTE_LENGTH - 4]))
            .build();

    assertThrows(TypeConversionException.class, () -> ProtoConverter.convert(value));
  }

  @Test
  public void unrecognizedProtoValue() {
    int illegalValue = 128;
    ProtoValue invalid = ProtoValue.newBuilder().setValuelessValue(illegalValue).build();

    assertThrows(TypeConversionException.class, () -> ProtoConverter.convert(invalid));
  }

  @Test
  public void docWithRepeatedFieldName() throws TypeConversionException {
    ProtoValue protoValue =
        ProtoValue.newBuilder()
            .setDocument(
                ProtoDocument.newBuilder()
                    .addField(
                        ProtoDocument.Field.newBuilder()
                            .setName("value")
                            .setValue(ProtoValue.newBuilder().setInt32(1)))
                    .addField(
                        ProtoDocument.Field.newBuilder()
                            .setName("value")
                            .setValue(ProtoValue.newBuilder().setInt32(2))))
            .build();

    assertEquals(
        2, ProtoConverter.convert(protoValue).asDocument().get("value").asInt32().getValue());
  }

  @Test
  public void rawDocWithRepeatedFieldName() throws TypeConversionException {
    var bsonOutput = new BasicOutputBuffer();
    try (var bsonWriter = new BsonBinaryWriter(bsonOutput)) {
      bsonWriter.writeStartDocument();
      bsonWriter.writeInt32("value", 1);
      bsonWriter.writeInt32("value", 2);
      bsonWriter.writeEndDocument();
    }
    var bsonValue = new RawBsonDocument(bsonOutput.getInternalBuffer());

    ProtoValue protoValue =
        ProtoValue.newBuilder()
            .setDocument(
                ProtoDocument.newBuilder()
                    .addField(
                        ProtoDocument.Field.newBuilder()
                            .setName("value")
                            .setValue(ProtoValue.newBuilder().setInt32(2))))
            .build();

    assertEquals(protoValue, ProtoConverter.convert(bsonValue));
  }
}
