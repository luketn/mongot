package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;

import com.google.protobuf.compiler.PluginProtos;
import org.bson.BsonType;

public class ProtoSymbolCodeGenerator implements CodeGenerator {
  private final Descriptor descriptor;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();

  public ProtoSymbolCodeGenerator(Descriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public Iterable<PluginProtos.CodeGeneratorResponse.File> generate() {
    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      writeBsonToScope.writeLine("writer.writeSymbol(getRep());");
    }

    try (var toBsonScope =
        this.messageOutput.openScope("@Override public org.bson.BsonSymbol toBson()")) {
      toBsonScope.writeLine("return new org.bson.BsonSymbol(getRep());");
    }

    CodeGeneratorUtils.generateBsonValueTypes(BsonType.SYMBOL, this.messageOutput.getRootScope());

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReaderValidatedType(
            this.descriptor, BsonType.SYMBOL, this.builderOutput)) {
      mergeBsonFromScope.writeLine("return setRep(reader.readSymbol());");
    }

    try (var mergeBsonValue =
        CodeGeneratorUtils.openBsonValueMergeScope(
            this.descriptor, BsonType.SYMBOL, this.builderOutput, this.messageOutput)) {
      mergeBsonValue.writeLine("return setRep(value.getSymbol());");
    }

    return CodeGeneratorUtils.valueMessageFiles(
        this.descriptor, this.messageOutput, this.builderOutput);
  }
}
