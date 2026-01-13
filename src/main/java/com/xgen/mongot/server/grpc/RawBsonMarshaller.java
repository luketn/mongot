package com.xgen.mongot.server.grpc;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import java.io.InputStream;
import org.bson.BsonBinaryWriter;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;

/** This marshaller will be called by gRPC libraries to encode/decode {@link RawBsonDocument}. */
public class RawBsonMarshaller implements MethodDescriptor.Marshaller<RawBsonDocument> {

  private static final EncoderContext ENCODER_CONTEXT = EncoderContext.builder().build();

  private static final CodecRegistry CODEC_REGISTRY =
      CodecRegistries.fromProviders(new BsonValueCodecProvider());

  @Override
  public InputStream stream(RawBsonDocument value) {
    try {
      int size = value.getByteBuffer().remaining() + 1;

      ByteBuf out = ByteBufAllocator.DEFAULT.buffer(size);
      BasicOutputBuffer buffer = new BasicOutputBuffer();
      Codec<RawBsonDocument> codec = CODEC_REGISTRY.get(RawBsonDocument.class);

      try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
        codec.encode(writer, value, ENCODER_CONTEXT);
      }

      out.writeBytes(buffer.getInternalBuffer(), 0, buffer.getSize());

      return new ByteBufInputStream(out, true);

    } catch (Exception e) {
      throw Status.INTERNAL
          .withDescription("cannot encode RawBsonDocument")
          .withCause(e)
          .asRuntimeException();
    }
  }

  @Override
  public RawBsonDocument parse(InputStream stream) {
    try {
      return new RawBsonDocument(stream.readAllBytes());
    } catch (Exception e) {
      throw Status.INTERNAL
          .withDescription("cannot decode RawBsonDocument")
          .withCause(e)
          .asRuntimeException();
    }
  }
}
