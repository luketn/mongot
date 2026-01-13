package com.xgen.proto.plugin;

import com.google.common.base.CaseFormat;
import com.google.protobuf.Descriptors.Descriptor;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.bson.BsonType;

public enum WellKnownTypes {
  DOCUMENT(BsonType.DOCUMENT, ProtoDocumentCodeGenerator::new),
  ARRAY(BsonType.ARRAY, ProtoArrayCodeGenerator::new),
  VALUE(EnumSet.allOf(BsonType.class), ProtoValueCodeGenerator::new),
  BINARY_DATA(BsonType.BINARY, ProtoBinaryDataCodeGenerator::new),
  DATE_TIME(BsonType.DATE_TIME, ProtoDateTimeCodeGenerator::new),
  TIMESTAMP(BsonType.TIMESTAMP, ProtoTimestampCodeGenerator::new),
  OBJECT_ID(BsonType.OBJECT_ID, ProtoObjectIdCodeGenerator::new),
  DECIMAL128(BsonType.DECIMAL128, ProtoDecimal128CodeGenerator::new),
  REGULAR_EXPRESSION(BsonType.REGULAR_EXPRESSION, ProtoRegularExpressionCodeGenerator::new),
  DB_POINTER(BsonType.DB_POINTER, ProtoDbPointerCodeGenerator::new),
  SYMBOL(BsonType.SYMBOL, ProtoSymbolCodeGenerator::new),
  JAVA_SCRIPT(BsonType.JAVASCRIPT, ProtoJavaScriptCodeGenerator::new),
  JAVA_SCRIPT_WITH_SCOPE(
      BsonType.JAVASCRIPT_WITH_SCOPE, ProtoJavaScriptWithScopeCodeGenerator::new);

  private static final String WELL_KNOWN_MESSAGE_CLASS_PREFIX = "com.xgen.mongot.proto.bson.Proto";

  WellKnownTypes(BsonType bsonType, Function<Descriptor, CodeGenerator> makeCodeGenerator) {
    this(EnumSet.of(bsonType), makeCodeGenerator);
  }

  WellKnownTypes(
      EnumSet<BsonType> bsonTypes, Function<Descriptor, CodeGenerator> makeCodeGenerator) {
    this.bsonTypes = bsonTypes;
    this.messageClassName =
        WELL_KNOWN_MESSAGE_CLASS_PREFIX
            + CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, this.name());
    this.makeCodeGenerator = makeCodeGenerator;
  }

  /**
   * Gets the well-known type for messageDescriptor.
   *
   * @param messageDescriptor
   * @return the corresponding enum value, or empty if this type is not well-known.
   */
  public static Optional<WellKnownTypes> get(Descriptor messageDescriptor) {
    return Optional.ofNullable(NAME_MAP.get(messageDescriptor.getFullName()));
  }

  /**
   * @return the set of bson types that this value message may contain.
   */
  public EnumSet<BsonType> getBsonTypes() {
    return this.bsonTypes;
  }

  /**
   * @return the full qualified name of the well-known type message descriptor.
   */
  public String getMessageClassName() {
    return this.messageClassName;
  }

  /**
   * Create a new CodeGenerator.
   *
   * @param messageDescriptor corresponding to this well-known type.
   * @return the code generator.
   */
  public CodeGenerator newCodeGenerator(Descriptor messageDescriptor) {
    return this.makeCodeGenerator.apply(messageDescriptor);
  }

  private static final Map<String, WellKnownTypes> NAME_MAP =
      Arrays.stream(WellKnownTypes.values())
          .collect(Collectors.toUnmodifiableMap(wkt -> wkt.getMessageClassName(), wkt -> wkt));

  private final EnumSet<BsonType> bsonTypes;
  private final String messageClassName;
  private final Function<Descriptor, CodeGenerator> makeCodeGenerator;
}
