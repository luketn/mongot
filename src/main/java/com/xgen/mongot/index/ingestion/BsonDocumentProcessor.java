package com.xgen.mongot.index.ingestion;

import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.index.ingestion.parsers.GeometryParser;
import com.xgen.mongot.index.ingestion.parsers.KnnVectorParser;
import com.xgen.mongot.util.bson.BsonVectorParser;
import com.xgen.mongot.util.bson.Vector;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.bson.BSONException;
import org.bson.BsonBinary;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinarySubType;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.ByteBufferBsonInput;
import org.bson.types.Binary;

/**
 * Responsible for traversing a BsonDocument and deserializing values that {@link DocumentHandler}
 * indicates it is interested in. Stateless, static class that is only responsible for deserializing
 * BSON and passing deserialized BSON values to indexable value builder.
 */
public class BsonDocumentProcessor {

  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();
  private static final CodecRegistry CODEC_REGISTRY =
      CodecRegistries.fromProviders(new BsonValueCodecProvider());

  public static void process(RawBsonDocument bsonDocument, DocumentHandler documentHandler)
      throws IOException {
    try (BsonBinaryReader bsonReader =
        new BsonBinaryReader(new ByteBufferBsonInput(bsonDocument.getByteBuffer()))) {
      handleDocumentField(bsonReader, documentHandler);
    }
  }

  private static void handleDocumentField(
      BsonBinaryReader bsonReader, DocumentHandler documentHandler) throws IOException {
    bsonReader.readStartDocument();

    while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      Optional<FieldValueHandler> maybeHandler =
          documentHandler.valueHandler(bsonReader.readName());
      if (maybeHandler.isPresent()) {
        FieldValueHandler handler = maybeHandler.get();
        handler.markFieldNameExists();
        handleField(bsonReader, handler);
      } else {
        bsonReader.skipValue();
      }
    }

    bsonReader.readEndDocument();
  }

  private static void handleField(BsonBinaryReader bsonReader, FieldValueHandler fieldValueHandler)
      throws IOException {
    // The bson type of the next value is indexed, so deserialize it.
    switch (bsonReader.getCurrentBsonType()) {
      case ARRAY -> {
        // Check if we should iterate the BsonArray for each deserialization strategy.
        try (var supplier = ResettingLazySupplier.create(bsonReader, KnnVectorParser::parse)) {
          fieldValueHandler.handleKnnVector(supplier);
        }

        Optional<FieldValueHandler> maybeHandler = fieldValueHandler.arrayFieldValueHandler();
        if (maybeHandler.isPresent()) {
          FieldValueHandler handler = maybeHandler.get();
          bsonReader.readStartArray();
          while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            handleField(bsonReader, handler);
          }
          bsonReader.readEndArray();
        } else {
          bsonReader.skipValue();
        }
      }
      case DOCUMENT -> {
        // Check if we should deserialize the BsonDocument for each deserialization strategy.
        try (var supplier = ResettingLazySupplier.create(bsonReader, GeometryParser::parse)) {
          fieldValueHandler.handleGeometry(supplier);
        }
        Optional<DocumentHandler> maybeDocumentHandler = fieldValueHandler.subDocumentHandler();
        if (maybeDocumentHandler.isPresent()) {
          DocumentHandler documentHandler = maybeDocumentHandler.get();
          handleDocumentField(bsonReader, documentHandler);
        } else {
          bsonReader.skipValue();
        }
      }
      case BINARY -> {
        byte binarySubtype = bsonReader.peekBinarySubType();
        if (binarySubtype == BsonBinarySubType.UUID_STANDARD.getValue()) {
          try (var supplier =
              ResettingLazySupplier.create(bsonReader, BsonDocumentProcessor::readUuid)) {
            // If the binary value is a UUID v4, it will be indexed
            fieldValueHandler.handleUuid(supplier);
          }
        }

        if (binarySubtype == BsonVectorParser.VECTOR_SUB_TYPE) {
          try (var supplier =
              ResettingLazySupplier.create(bsonReader, BsonDocumentProcessor::readBsonVector)) {
            fieldValueHandler.handleKnnVector(supplier);
          }
        }

        try (var supplier =
            SkippingLazySupplier.create(bsonReader, BsonDocumentProcessor::readBinary)) {
          // Will store the binary value, but not index it
          fieldValueHandler.handleBinary(supplier);
        }
      }
      case BOOLEAN -> {
        try (var supplier = SkippingLazySupplier.create(bsonReader, BsonReader::readBoolean)) {
          fieldValueHandler.handleBoolean(supplier);
        }
      }
      case DATE_TIME -> {
        try (var supplier = SkippingLazySupplier.create(bsonReader, BsonReader::readDateTime)) {
          fieldValueHandler.handleDateTime(supplier);
        }
      }
      case DOUBLE -> {
        try (var supplier = SkippingLazySupplier.create(bsonReader, BsonReader::readDouble)) {
          fieldValueHandler.handleDouble(supplier);
        }
      }
      case INT32 -> {
        try (var supplier = SkippingLazySupplier.create(bsonReader, BsonReader::readInt32)) {
          fieldValueHandler.handleInt32(supplier);
        }
      }
      case INT64 -> {
        try (var supplier = SkippingLazySupplier.create(bsonReader, BsonReader::readInt64)) {
          fieldValueHandler.handleInt64(supplier);
        }
      }
      case OBJECT_ID -> {
        try (var supplier = SkippingLazySupplier.create(bsonReader, BsonReader::readObjectId)) {
          fieldValueHandler.handleObjectId(supplier);
        }
      }
      case STRING -> {
        try (var supplier = SkippingLazySupplier.create(bsonReader, BsonReader::readString)) {
          fieldValueHandler.handleString(supplier);
        }
      }
      case NULL -> {
        // Read the null value eagerly since its value will not be used for indexing
        bsonReader.readNull();

        fieldValueHandler.handleNull();
      }
      default -> {
        try (var supplier =
            SkippingLazySupplier.create(bsonReader, BsonDocumentProcessor::bsonValueReader)) {
          fieldValueHandler.handleRawBsonValue(supplier);
        }
      }
    }
  }

  private static BsonValue bsonValueReader(BsonReader bsonReader) {
    return CODEC_REGISTRY
        .get(BsonValueCodecProvider.getClassForBsonType(bsonReader.getCurrentBsonType()))
        .decode(bsonReader, DecoderContext.builder().build());
  }

  private static Binary readBinary(BsonReader bsonReader) {
    BsonBinary bsonBinary = bsonReader.readBinaryData();
    return new Binary(bsonBinary.getType(), bsonBinary.getData());
  }

  private static Optional<Vector> readBsonVector(BsonReader bsonReader) {
    BsonBinary bsonBinary = bsonReader.readBinaryData();
    try {
      return Optional.of(BsonVectorParser.parse(bsonBinary));
    } catch (BSONException e) {
      FLOGGER.atWarning().atMostEvery(10, TimeUnit.MINUTES).log(
          "Error parsing BSON vector with exception: %s", e);
    }
    return Optional.empty();
  }

  private static Optional<UUID> readUuid(BsonReader bsonReader) {
    try {
      return Optional.of(bsonReader.readBinaryData().asUuid());
    } catch (BSONException e) {
      // Swallow exception when uuid is malformed
    }

    return Optional.empty();
  }
}
