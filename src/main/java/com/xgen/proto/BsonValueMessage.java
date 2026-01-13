package com.xgen.proto;

import com.google.protobuf.Message;
import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.BsonWriter;

/**
 * Bson extensions for a value Message.
 *
 * <p>Value messages differ from regular messages in that they represent a non-document Bson type
 * that cannot be encoded on its own.
 */
public interface BsonValueMessage extends Message {
  @Override
  BsonValueBuilder newBuilderForType();

  @Override
  BsonValueBuilder toBuilder();

  /**
   * Write this message as a single value to writer.
   *
   * <p>We assume at least one of the following:
   *
   * <ol>
   *   <ul>
   *     this object represents a document type ({@link BsonMessage})
   *   </ul>
   *   <ul>
   *     the field name has already been written and writer is positioned to emit a value.
   *   </ul>
   * </ol>
   *
   * @param writer to emit to.
   */
  void writeBsonTo(BsonWriter writer);

  /**
   * Convert a copy of the contents of this object as a BsonValue.
   *
   * @return the value
   */
  BsonValue toBson();

  interface BsonValueBuilder extends Builder {
    @Override
    BsonValueMessage build();

    @Override
    BsonValueMessage buildPartial();

    /**
     * Merge the value in reader into this message.
     *
     * @param reader bson stream to read from.
     * @return this
     * @throws BsonProtoParseException if contents of the reader does not conform to the schema set
     *     by the protobuf message descriptor.
     */
    BsonValueBuilder mergeBsonFrom(BsonReader reader) throws BsonProtoParseException;

    /**
     * Merge value into this message.
     *
     * @param value Bson value to merge.
     * @return this
     * @throws BsonProtoParseException if the value does not conform to the schema set by the
     *     protobuf message descriptor.
     */
    BsonValueBuilder mergeBsonFrom(BsonValue value) throws BsonProtoParseException;
  }
}
