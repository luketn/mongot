package com.xgen.mongot.config.provider.community;

import com.google.common.net.HostAndPort;
import com.xgen.mongot.config.provider.community.parser.PathField;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import com.xgen.mongot.util.mongodb.Databases;
import java.nio.file.Path;
import java.util.List;
import org.bson.BsonDocument;

public record RouterConfig(
    List<HostAndPort> hostandPorts,
    String username,
    Path passwordFile,
    String authSource,
    boolean tls,
    MongoReadPreferenceName readPreference)
    implements DocumentEncodable {
  private static class Fields {
    public static final Field.Required<List<String>> HOST_AND_PORT =
        Field.builder("hostAndPort")
            .singleValueOrListOf(Value.builder().stringValue().mustNotBeEmpty().required())
            .mustNotBeEmpty()
            .required();

    public static final Field.Required<String> USERNAME =
        Field.builder("username").stringField().mustNotBeEmpty().required();

    public static final Field.Required<Path> PASSWORD_FILE =
        Field.builder("passwordFile").classField(PathField.PARSER, PathField.ENCODER).required();

    public static final Field.WithDefault<String> AUTH_SOURCE =
        Field.builder("authSource")
            .stringField()
            .mustNotBeEmpty()
            .optional()
            .withDefault(Databases.ADMIN);

    public static final Field.WithDefault<Boolean> TLS =
        Field.builder("tls").booleanField().optional().withDefault(false);

    public static final Field.WithDefault<MongoReadPreferenceName> READ_PREFERENCE =
        Field.builder("readPreference")
            .enumField(MongoReadPreferenceName.class)
            .asCamelCase()
            .optional()
            .withDefault(MongoReadPreferenceName.PRIMARY);
  }

  public static RouterConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new RouterConfig(
        parser.getField(Fields.HOST_AND_PORT).unwrap().stream()
            .map(HostAndPort::fromString)
            .toList(),
        parser.getField(Fields.USERNAME).unwrap(),
        parser.getField(Fields.PASSWORD_FILE).unwrap(),
        parser.getField(Fields.AUTH_SOURCE).unwrap(),
        parser.getField(Fields.TLS).unwrap(),
        parser.getField(Fields.READ_PREFERENCE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.HOST_AND_PORT, this.hostandPorts.stream().map(HostAndPort::toString).toList())
        .field(Fields.USERNAME, this.username)
        .field(Fields.PASSWORD_FILE, this.passwordFile)
        .field(Fields.AUTH_SOURCE, this.authSource)
        .field(Fields.TLS, this.tls)
        .field(Fields.READ_PREFERENCE, this.readPreference)
        .build();
  }
}
