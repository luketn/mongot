package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;

import com.google.protobuf.compiler.PluginProtos;
import org.bson.BsonType;

public class ProtoRegularExpressionCodeGenerator implements CodeGenerator {
  private final Descriptor descriptor;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();

  public ProtoRegularExpressionCodeGenerator(Descriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public Iterable<PluginProtos.CodeGeneratorResponse.File> generate() {
    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      writeBsonToScope.writeLine("writer.writeRegularExpression(toBson());");
    }

    try (var toBsonScope =
        this.messageOutput.openScope("@Override public org.bson.BsonRegularExpression toBson()")) {
      toBsonScope.writeLine(
          "return new org.bson.BsonRegularExpression(getPattern(), getOptions());");
    }

    CodeGeneratorUtils.generateBsonValueTypes(
        BsonType.REGULAR_EXPRESSION, this.messageOutput.getRootScope());

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReaderValidatedType(
            this.descriptor, BsonType.REGULAR_EXPRESSION, this.builderOutput)) {
      mergeBsonFromScope.writeLine("return mergeBsonFrom(reader.readRegularExpression());");
    }

    try (var mergeBsonValue =
        CodeGeneratorUtils.openBsonValueMergeScope(
            this.descriptor, BsonType.REGULAR_EXPRESSION, this.builderOutput, this.messageOutput)) {
      mergeBsonValue.writeLine(
          "return setPattern(value.getPattern()).setOptions(value.getOptions());");
    }

    return CodeGeneratorUtils.valueMessageFiles(
        this.descriptor, this.messageOutput, this.builderOutput);
  }
}
