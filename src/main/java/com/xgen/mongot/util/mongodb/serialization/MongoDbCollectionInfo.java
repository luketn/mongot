package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public sealed interface MongoDbCollectionInfo
    permits MongoDbCollectionInfo.Collection, MongoDbCollectionInfo.View {

  Logger LOG = LoggerFactory.getLogger(MongoDbCollectionInfo.class);

  class Fields {
    public static final Field.Optional<Type> TYPE =
        Field.builder("type").enumField(Type.class).asCamelCase().optional().noDefault();
    public static final Field.Required<String> NAME =
        Field.builder("name").stringField().required();
  }

  enum Type {
    COLLECTION,
    VIEW,
    TIMESERIES
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  static Optional<MongoDbCollectionInfo> fromBsonDocument(BsonDocument document) {
    try (var parser = BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {

      var type = parser.getField(Fields.TYPE).unwrap();

      if (type.isEmpty()) {
        return Optional.empty();
      }

      return switch (type.get()) {
        case COLLECTION -> Collection.fromBson(parser);
        case VIEW -> View.fromBson(parser);
        case TIMESERIES ->
            // mongot does not support timeseries collections
            // so don't bother deserializing them
            Optional.empty();
      };

    } catch (BsonParseException e) {
      LOG.atError()
          .addKeyValue("document", document)
          .log("Cannot deserialize collection info document, skipping it");
      return Optional.empty();
    }
  }

  record Collection(String name, MongoDbCollectionInfo.Collection.Info info)
      implements MongoDbCollectionInfo {

    public record Info(UUID uuid) implements DocumentEncodable {
      private static class Fields {
        public static final Field.Required<UUID> UUID =
            Field.builder("uuid").uuidField().required();
      }

      private static Info fromBson(DocumentParser parser) throws BsonParseException {
        return new Info(parser.getField(Fields.UUID).unwrap());
      }

      @Override
      public BsonDocument toBson() {
        return BsonDocumentBuilder.builder().field(Fields.UUID, this.uuid).build();
      }
    }

    private static class Fields {
      public static final Field.Required<Info> INFO =
          Field.builder("info").classField(Info::fromBson).allowUnknownFields().required();
    }

    private static Optional<MongoDbCollectionInfo> fromBson(DocumentParser parser)
        throws BsonParseException {
      return Optional.of(
          new Collection(
              parser.getField(MongoDbCollectionInfo.Fields.NAME).unwrap(),
              parser.getField(Fields.INFO).unwrap()));
    }

    @Override
    public Type getType() {
      return Type.COLLECTION;
    }
  }

  record View(String name, MongoDbCollectionInfo.View.Options options)
      implements MongoDbCollectionInfo {
    public record Options(String viewOn, List<BsonDocument> pipeline) implements DocumentEncodable {
      private static class Fields {
        public static final Field.Required<String> VIEW_ON =
            Field.builder("viewOn").stringField().mustNotBeBlank().required();
        public static final Field.Required<List<BsonDocument>> PIPELINE =
            Field.builder("pipeline").listOf(Value.builder().documentValue().required()).required();
      }

      private static Options fromBson(DocumentParser parser) throws BsonParseException {
        return new Options(
            parser.getField(Fields.VIEW_ON).unwrap(), parser.getField(Fields.PIPELINE).unwrap());
      }

      @Override
      public BsonDocument toBson() {
        return BsonDocumentBuilder.builder()
            .field(Fields.VIEW_ON, this.viewOn)
            .field(Fields.PIPELINE, this.pipeline)
            .build();
      }
    }

    private static class Fields {
      public static final Field.Required<Options> OPTIONS =
          Field.builder("options").classField(Options::fromBson).allowUnknownFields().required();
    }

    private static Optional<MongoDbCollectionInfo> fromBson(DocumentParser parser)
        throws BsonParseException {
      return Optional.of(
          new View(
              parser.getField(MongoDbCollectionInfo.Fields.NAME).unwrap(),
              parser.getField(Fields.OPTIONS).unwrap()));
    }

    @Override
    public Type getType() {
      return Type.VIEW;
    }
  }

  Type getType();

  String name();
}
