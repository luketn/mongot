package com.xgen.mongot.util.bson;

import java.util.Optional;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

final class OptionalCodec<T> implements Codec<Optional<T>> {

  private final Class<Optional<T>> encoderClass;
  private final Codec<T> codec;

  OptionalCodec(Class<Optional<T>> encoderClass, Codec<T> codec) {
    this.encoderClass = encoderClass;
    this.codec = codec;
  }

  @Override
  public void encode(BsonWriter writer, Optional<T> optionalValue, EncoderContext encoderContext) {
    if (optionalValue != null && optionalValue.isPresent()) {
      this.codec.encode(writer, optionalValue.get(), encoderContext);
    } else {
      // we have to write something here - the name for this field is already written.
      writer.writeNull();
    }
  }

  @Override
  public Optional<T> decode(BsonReader reader, DecoderContext context) {
    if (reader.getCurrentBsonType() == BsonType.NULL) {
      return Optional.empty();
    }
    return Optional.ofNullable(this.codec.decode(reader, context));
  }

  @Override
  public Class<Optional<T>> getEncoderClass() {
    return this.encoderClass;
  }
}
