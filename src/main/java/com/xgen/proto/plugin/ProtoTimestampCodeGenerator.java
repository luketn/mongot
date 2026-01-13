package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;

import com.google.protobuf.compiler.PluginProtos;
import org.bson.BsonType;

public class ProtoTimestampCodeGenerator implements CodeGenerator {
  private final Descriptor descriptor;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();

  public ProtoTimestampCodeGenerator(Descriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public Iterable<PluginProtos.CodeGeneratorResponse.File> generate() {
    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      writeBsonToScope.writeLine("writer.writeTimestamp(new org.bson.BsonTimestamp(getRep()));");
    }

    try (var toBsonScope =
        this.messageOutput.openScope("@Override public org.bson.BsonTimestamp toBson()")) {
      toBsonScope.writeLine("return new org.bson.BsonTimestamp(getRep());");
    }

    CodeGeneratorUtils.generateBsonValueTypes(
        BsonType.TIMESTAMP, this.messageOutput.getRootScope());

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReaderValidatedType(
            this.descriptor, BsonType.TIMESTAMP, this.builderOutput)) {
      mergeBsonFromScope.writeLine("return mergeBsonFrom(reader.readTimestamp());");
    }

    try (var mergeBsonValue =
        CodeGeneratorUtils.openBsonValueMergeScope(
            this.descriptor, BsonType.TIMESTAMP, this.builderOutput, this.messageOutput)) {
      mergeBsonValue.writeLine("return setRep(value.getValue());");
    }

    return CodeGeneratorUtils.valueMessageFiles(
        this.descriptor, this.messageOutput, this.builderOutput);
  }
}
