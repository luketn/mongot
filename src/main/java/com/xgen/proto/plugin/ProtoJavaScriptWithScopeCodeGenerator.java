package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;

import com.google.protobuf.compiler.PluginProtos;
import org.bson.BsonType;

public class ProtoJavaScriptWithScopeCodeGenerator implements CodeGenerator {
  private final Descriptor descriptor;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();

  public ProtoJavaScriptWithScopeCodeGenerator(Descriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public Iterable<PluginProtos.CodeGeneratorResponse.File> generate() {
    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      writeBsonToScope.writeLine("writer.writeJavaScriptWithScope(getCode());");
      writeBsonToScope.writeLine("getScope().writeBsonTo(writer);");
    }

    try (var toBsonScope =
        this.messageOutput.openScope(
            "@Override public org.bson.BsonJavaScriptWithScope toBson()")) {
      toBsonScope.writeLine(
          "return new org.bson.BsonJavaScriptWithScope(getCode(), getScope().toBson());");
    }

    CodeGeneratorUtils.generateBsonValueTypes(
        BsonType.JAVASCRIPT_WITH_SCOPE, this.messageOutput.getRootScope());

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReaderValidatedType(
            this.descriptor, BsonType.JAVASCRIPT_WITH_SCOPE, this.builderOutput)) {
      mergeBsonFromScope.writeLine("setCode(reader.readJavaScriptWithScope());");
      mergeBsonFromScope.writeLine("getScopeBuilder().mergeBsonFromTypeValidated(reader);");
      mergeBsonFromScope.writeLine("return this;");
    }

    try (var mergeBsonValue =
        CodeGeneratorUtils.openBsonValueMergeScope(
            this.descriptor,
            BsonType.JAVASCRIPT_WITH_SCOPE,
            this.builderOutput,
            this.messageOutput)) {
      mergeBsonValue.writeLine("getScopeBuilder().mergeBsonFrom(value.getScope());");
      mergeBsonValue.writeLine("return setCode(value.getCode());");
    }

    return CodeGeneratorUtils.valueMessageFiles(
        this.descriptor, this.messageOutput, this.builderOutput);
  }
}
