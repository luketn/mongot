package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;

import com.google.protobuf.compiler.PluginProtos;
import org.bson.BsonType;

public class ProtoDbPointerCodeGenerator implements CodeGenerator {
  private final Descriptor descriptor;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();

  public ProtoDbPointerCodeGenerator(Descriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public Iterable<PluginProtos.CodeGeneratorResponse.File> generate() {
    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      writeBsonToScope.writeLine("writer.writeDBPointer(toBson());");
    }

    try (var toBsonScope =
        this.messageOutput.openScope("@Override public org.bson.BsonDbPointer toBson()")) {
      toBsonScope.writeLine(
          "return new org.bson.BsonDbPointer(getNamespace(), getObjectId().toObjectId());");
    }

    CodeGeneratorUtils.generateBsonValueTypes(
        BsonType.DB_POINTER, this.messageOutput.getRootScope());

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReaderValidatedType(
            this.descriptor, BsonType.DB_POINTER, this.builderOutput)) {
      mergeBsonFromScope.writeLine("return mergeBsonFrom(reader.readDBPointer());");
    }

    try (var mergeBsonValue =
        CodeGeneratorUtils.openBsonValueMergeScope(
            this.descriptor, BsonType.DB_POINTER, this.builderOutput, this.messageOutput)) {
      mergeBsonValue.writeLine(
          "getObjectIdBuilder().setRep(com.google.protobuf.ByteString.copyFrom(value.getId().toByteArray()));");
      mergeBsonValue.writeLine("return setNamespace(value.getNamespace());");
    }

    return CodeGeneratorUtils.valueMessageFiles(
        this.descriptor, this.messageOutput, this.builderOutput);
  }
}
