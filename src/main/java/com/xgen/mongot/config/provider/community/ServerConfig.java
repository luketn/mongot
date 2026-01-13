package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.community.parser.PathField;
import com.xgen.mongot.config.util.TlsMode;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;

public record ServerConfig(GrpcServerConfig grpc) implements DocumentEncodable {
  private static class Fields {
    public static final Field.Required<GrpcServerConfig> GRPC =
        Field.builder("grpc")
            .classField(GrpcServerConfig::fromBson)
            .disallowUnknownFields()
            .required();
  }

  public static ServerConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new ServerConfig(parser.getField(Fields.GRPC).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.GRPC, this.grpc).build();
  }

  public TlsMode getGrpcTlsMode() {
    return this.grpc.tls().map(GrpcServerConfig.GrpcTls::mode).orElse(TlsMode.DISABLED);
  }

  public record GrpcServerConfig(String address, Optional<GrpcServerConfig.GrpcTls> tls)
      implements DocumentEncodable {
    private static class Fields {
      public static final Field.Required<String> ADDRESS =
          Field.builder("address").stringField().required();
      public static final Field.Optional<GrpcServerConfig.GrpcTls> TLS =
          Field.builder("tls")
              .classField(GrpcServerConfig.GrpcTls::fromBson)
              .disallowUnknownFields()
              .optional()
              .noDefault();
    }

    public record GrpcTls(TlsMode mode, Optional<Path> certificateKeyFile, Optional<Path> caFile)
        implements DocumentEncodable {
      private static class Fields {
        public static final Field.Required<TlsMode> MODE =
            Field.builder("mode").enumField(TlsMode.class).asCaseInsensitive().required();
        public static final Field.Optional<Path> CERTIFICATE_KEY_FILE =
            Field.builder("certificateKeyFile")
                .classField(PathField.PARSER, PathField.ENCODER)
                .optional()
                .noDefault();
        public static final Field.Optional<Path> CERTIFICATE_AUTHORITY_FILE =
            Field.builder("caFile")
                .classField(PathField.PARSER, PathField.ENCODER)
                .optional()
                .noDefault();
      }

      public static GrpcServerConfig.GrpcTls fromBson(DocumentParser parser)
          throws BsonParseException {
        TlsMode mode = parser.getField(GrpcServerConfig.GrpcTls.Fields.MODE).unwrap();
        Optional<Path> certificateKeyFile = parser.getField(Fields.CERTIFICATE_KEY_FILE).unwrap();
        Optional<Path> caFile = parser.getField(Fields.CERTIFICATE_AUTHORITY_FILE).unwrap();
        // Validate requirement for certificateKeyFile when TlsMode is enabled
        if (mode != TlsMode.DISABLED && certificateKeyFile.isEmpty()) {
          parser
              .getContext()
              .handleSemanticError("certificateKeyFile is required when tls is enabled");
        }
        // Validate requirement for caFile when mTlsMode is enabled
        if (mode == TlsMode.MTLS && caFile.isEmpty()) {
          parser.getContext().handleSemanticError("caFile is required when mtls is enabled");
        }
        return new GrpcServerConfig.GrpcTls(
            parser.getField(Fields.MODE).unwrap(),
            parser.getField(Fields.CERTIFICATE_KEY_FILE).unwrap(),
            parser.getField(Fields.CERTIFICATE_AUTHORITY_FILE).unwrap());
      }

      @Override
      public BsonDocument toBson() {
        return BsonDocumentBuilder.builder()
            .field(Fields.MODE, this.mode)
            .field(Fields.CERTIFICATE_KEY_FILE, this.certificateKeyFile)
            .field(Fields.CERTIFICATE_AUTHORITY_FILE, this.caFile)
            .build();
      }
    }

    public static GrpcServerConfig fromBson(DocumentParser parser) throws BsonParseException {
      return new GrpcServerConfig(
          parser.getField(Fields.ADDRESS).unwrap(), parser.getField(Fields.TLS).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.ADDRESS, this.address)
          .field(Fields.TLS, this.tls)
          .build();
    }
  }

  public Optional<Path> getGrpcCertificateKeyFile() {
    return this.grpc.tls().flatMap(GrpcServerConfig.GrpcTls::certificateKeyFile);
  }

  public Optional<Path> getGrpcCaFile() {
    return this.grpc.tls().flatMap(GrpcServerConfig.GrpcTls::caFile);
  }
}
