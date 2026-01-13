package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;

import com.google.protobuf.compiler.PluginProtos;
import org.bson.BsonType;

public class ProtoDateTimeCodeGenerator implements CodeGenerator {
  private final Descriptor descriptor;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();

  public ProtoDateTimeCodeGenerator(Descriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public Iterable<PluginProtos.CodeGeneratorResponse.File> generate() {
    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      writeBsonToScope.writeLine("writer.writeDateTime(getRep());");
    }

    try (var toBsonScope =
        this.messageOutput.openScope("@Override public org.bson.BsonDateTime toBson()")) {
      toBsonScope.writeLine("return new org.bson.BsonDateTime(getRep());");
    }

    CodeGeneratorUtils.generateBsonValueTypes(
        BsonType.DATE_TIME, this.messageOutput.getRootScope());

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReaderValidatedType(
            this.descriptor, BsonType.DATE_TIME, this.builderOutput)) {
      mergeBsonFromScope.writeLine("return setRep(reader.readDateTime());");
    }

    try (var mergeBsonValue =
        CodeGeneratorUtils.openBsonValueMergeScope(
            this.descriptor, BsonType.DATE_TIME, this.builderOutput, this.messageOutput)) {
      mergeBsonValue.writeLine("return setRep(value.getValue());");
    }

    return CodeGeneratorUtils.valueMessageFiles(
        this.descriptor, this.messageOutput, this.builderOutput);
  }
}
