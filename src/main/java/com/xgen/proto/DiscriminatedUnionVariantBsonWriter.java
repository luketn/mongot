package com.xgen.proto;

import org.bson.BsonBinary;
import org.bson.BsonDbPointer;
import org.bson.BsonReader;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonWriter;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

/**
 * Wraps a BsonWriter to omit the outermost document delimiter. This is used for discriminated union
 * types where the variant message is written to the same document as the outer message.
 */
public class DiscriminatedUnionVariantBsonWriter implements BsonWriter {
  private final BsonWriter writer;
  private int documentLevel = 0;

  public DiscriminatedUnionVariantBsonWriter(BsonWriter writer) {
    this.writer = writer;
  }

  // On the outermost document skip writing the start document marker.
  // This happens when we call BsonMessage.writeBsonTo() for a variant in a discriminated union.
  @Override
  public void writeStartDocument() {
    if (this.documentLevel > 0) {
      this.writer.writeStartDocument();
    }
    this.documentLevel += 1;
  }

  // On the outermost document skip writing the start document marker.
  // This happens when we call BsonMessage.writeBsonTo() for a variant in a discriminated union.
  @Override
  public void writeStartDocument(String name) {
    if (this.documentLevel > 0) {
      this.writer.writeStartDocument(name);
    }
    this.documentLevel += 1;
  }

  // On the outermost document skip writing the end document marker.
  // This happens when we call BsonMessage.writeBsonTo() for a variant in a discriminated union.
  @Override
  public void writeEndDocument() {
    if (this.documentLevel > 1) {
      this.writer.writeEndDocument();
    }
    this.documentLevel -= 1;
  }

  // Methods below this point simply delegate to the underlying reader type.

  @Override
  public void writeStartArray() {
    this.writer.writeStartArray();
  }

  @Override
  public void writeStartArray(String name) {
    this.writer.writeStartArray(name);
  }

  @Override
  public void writeEndArray() {
    this.writer.writeEndArray();
  }

  @Override
  public void flush() {
    this.writer.flush();
  }

  @Override
  public void pipe(BsonReader reader) {
    this.writer.pipe(reader);
  }

  @Override
  public void writeBinaryData(BsonBinary binary) {
    this.writer.writeBinaryData(binary);
  }

  @Override
  public void writeBinaryData(String name, BsonBinary binary) {
    this.writer.writeBinaryData(name, binary);
  }

  @Override
  public void writeBoolean(boolean value) {
    this.writer.writeBoolean(value);
  }

  @Override
  public void writeBoolean(String name, boolean value) {
    this.writer.writeBoolean(name, value);
  }

  @Override
  public void writeDateTime(long value) {
    this.writer.writeDateTime(value);
  }

  @Override
  public void writeDateTime(String name, long value) {
    this.writer.writeDateTime(name, value);
  }

  @Override
  public void writeDBPointer(BsonDbPointer value) {
    this.writer.writeDBPointer(value);
  }

  @Override
  public void writeDBPointer(String name, BsonDbPointer value) {
    this.writer.writeDBPointer(name, value);
  }

  @Override
  public void writeDecimal128(Decimal128 value) {
    this.writer.writeDecimal128(value);
  }

  @Override
  public void writeDecimal128(String name, Decimal128 value) {
    this.writer.writeDecimal128(name, value);
  }

  @Override
  public void writeDouble(double value) {
    this.writer.writeDouble(value);
  }

  @Override
  public void writeDouble(String name, double value) {
    this.writer.writeDouble(name, value);
  }

  @Override
  public void writeInt32(int value) {
    this.writer.writeInt32(value);
  }

  @Override
  public void writeInt32(String name, int value) {
    this.writer.writeInt32(name, value);
  }

  @Override
  public void writeInt64(long value) {
    this.writer.writeInt64(value);
  }

  @Override
  public void writeInt64(String name, long value) {
    this.writer.writeInt64(name, value);
  }

  @Override
  public void writeJavaScript(String code) {
    this.writer.writeJavaScript(code);
  }

  @Override
  public void writeJavaScript(String name, String code) {
    this.writer.writeJavaScript(name, code);
  }

  @Override
  public void writeJavaScriptWithScope(String code) {
    this.writer.writeJavaScriptWithScope(code);
  }

  @Override
  public void writeJavaScriptWithScope(String name, String code) {
    this.writer.writeJavaScriptWithScope(name, code);
  }

  @Override
  public void writeMaxKey() {
    this.writer.writeMaxKey();
  }

  @Override
  public void writeMaxKey(String name) {
    this.writer.writeMaxKey(name);
  }

  @Override
  public void writeMinKey() {
    this.writer.writeMinKey();
  }

  @Override
  public void writeMinKey(String name) {
    this.writer.writeMinKey(name);
  }

  @Override
  public void writeName(String name) {
    this.writer.writeName(name);
  }

  @Override
  public void writeNull() {
    this.writer.writeNull();
  }

  @Override
  public void writeNull(String name) {
    this.writer.writeNull(name);
  }

  @Override
  public void writeObjectId(ObjectId objectId) {
    this.writer.writeObjectId(objectId);
  }

  @Override
  public void writeObjectId(String name, ObjectId objectId) {
    this.writer.writeObjectId(name, objectId);
  }

  @Override
  public void writeRegularExpression(BsonRegularExpression regularExpression) {
    this.writer.writeRegularExpression(regularExpression);
  }

  @Override
  public void writeRegularExpression(String name, BsonRegularExpression regularExpression) {
    this.writer.writeRegularExpression(name, regularExpression);
  }

  @Override
  public void writeString(String value) {
    this.writer.writeString(value);
  }

  @Override
  public void writeString(String name, String value) {
    this.writer.writeString(name, value);
  }

  @Override
  public void writeSymbol(String value) {
    this.writer.writeSymbol(value);
  }

  @Override
  public void writeSymbol(String name, String value) {
    this.writer.writeSymbol(name, value);
  }

  @Override
  public void writeTimestamp(BsonTimestamp value) {
    this.writer.writeTimestamp(value);
  }

  @Override
  public void writeTimestamp(String name, BsonTimestamp value) {
    this.writer.writeTimestamp(name, value);
  }

  @Override
  public void writeUndefined() {
    this.writer.writeUndefined();
  }

  @Override
  public void writeUndefined(String name) {
    this.writer.writeUndefined(name);
  }
}
