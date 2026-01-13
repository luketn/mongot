package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;

import com.google.protobuf.compiler.PluginProtos;
import org.bson.BsonType;

public class ProtoJavaScriptCodeGenerator implements CodeGenerator {
  private final Descriptor descriptor;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();

  public ProtoJavaScriptCodeGenerator(Descriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public Iterable<PluginProtos.CodeGeneratorResponse.File> generate() {
    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      writeBsonToScope.writeLine("writer.writeJavaScript(getCode());");
    }

    try (var toBsonScope =
        this.messageOutput.openScope("@Override public org.bson.BsonJavaScript toBson()")) {
      toBsonScope.writeLine("return new org.bson.BsonJavaScript(getCode());");
    }

    CodeGeneratorUtils.generateBsonValueTypes(
        BsonType.JAVASCRIPT, this.messageOutput.getRootScope());

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReaderValidatedType(
            this.descriptor, BsonType.JAVASCRIPT, this.builderOutput)) {
      mergeBsonFromScope.writeLine("return setCode(reader.readJavaScript());");
    }

    try (var mergeBsonValue =
        CodeGeneratorUtils.openBsonValueMergeScope(
            this.descriptor, BsonType.JAVASCRIPT, this.builderOutput, this.messageOutput)) {
      mergeBsonValue.writeLine("return setCode(value.getCode());");
    }

    return CodeGeneratorUtils.valueMessageFiles(
        this.descriptor, this.messageOutput, this.builderOutput);
  }
}
