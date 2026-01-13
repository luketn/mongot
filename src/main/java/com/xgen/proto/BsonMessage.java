package com.xgen.proto;

import java.nio.ByteBuffer;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.io.BasicOutputBuffer;

/** Bson extensions for Message */
public interface BsonMessage extends BsonValueMessage {
  @Override
  BsonBuilder newBuilderForType();

  @Override
  BsonBuilder toBuilder();

  @Override
  void writeBsonTo(BsonWriter writer);

  @Override
  default BsonDocument toBson() {
    var doc = new BsonDocument();
    writeBsonTo(new BsonDocumentWriter(doc));
    return doc;
  }

  default byte[] toBsonByteArray() {
    var output = new BasicOutputBuffer();
    writeBsonTo(new BsonBinaryWriter(output));
    return output.toByteArray();
  }

  interface BsonBuilder extends BsonValueMessage.BsonValueBuilder {
    @Override
    BsonMessage build();

    @Override
    BsonMessage buildPartial();

    /**
     * Merge the contents of reader into this message.
     *
     * @param reader bson stream to consume.
     * @return this
     * @throws BsonProtoParseException if an unknown field is encountered or the stream does not
     *     otherwise conform to the schema set by the protobuf message descriptor.
     */
    @Override
    BsonBuilder mergeBsonFrom(BsonReader reader) throws BsonProtoParseException;

    /**
     * Merge the contents of doc into this message.
     *
     * @param doc bson document to consume.
     * @return this
     * @throws BsonProtoParseException if an unknown field is encountered or the doc does not
     *     otherwise conform to the schema set by the protobuf message descriptor.
     */
    BsonBuilder mergeBsonFrom(BsonDocument doc) throws BsonProtoParseException;

    @Override
    BsonBuilder mergeBsonFrom(BsonValue value) throws BsonProtoParseException;

    /**
     * Merge the contents of bson binary encoded buf into this message.
     *
     * @param buf bson binary buffer to consume
     * @return this
     * @throws BsonProtoParseException if an unknown field is encountered or the buffer does not
     *     otherwise conform to the schema set by the protobuf message descriptor.
     */
    BsonBuilder mergeBsonFrom(ByteBuffer buf) throws BsonProtoParseException;

    /**
     * Merge the contents of reader into this message.
     *
     * @param reader bson stream to consume.
     * @param allowUnknownFields if set to true, field names that do not appear in the protobuf
     *     message descriptor are ignored instead of throwing an exception.
     * @return this
     * @throws BsonProtoParseException if the stream does not conform to the schema set by the
     *     protobuf message descriptor.
     */
    BsonBuilder mergeBsonFrom(BsonReader reader, boolean allowUnknownFields)
        throws BsonProtoParseException;
  }
}
