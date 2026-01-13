package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;

import com.google.protobuf.compiler.PluginProtos;
import org.bson.BsonType;

public class ProtoArrayCodeGenerator implements CodeGenerator {
  private final Descriptor descriptor;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();

  public ProtoArrayCodeGenerator(Descriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public Iterable<PluginProtos.CodeGeneratorResponse.File> generate() {
    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      writeBsonToScope.writeLine("writer.writeStartArray();");
      try (var valueScope = writeBsonToScope.openScope("for (var value : getValueList())")) {
        valueScope.writeLine("value.writeBsonTo(writer);");
      }
      writeBsonToScope.writeLine("writer.writeEndArray();");
    }

    try (var toBsonScope =
        this.messageOutput.openScope("@Override public org.bson.BsonArray toBson()")) {
      toBsonScope.writeLine("var array = new org.bson.BsonArray();");
      try (var valueScope = toBsonScope.openScope("for (var value : getValueList())")) {
        valueScope.writeLine("array.add(value.toBson());");
      }
      toBsonScope.writeLine("return array;");
    }

    CodeGeneratorUtils.generateBsonValueTypes(BsonType.ARRAY, this.messageOutput.getRootScope());

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReaderValidatedType(
            this.descriptor, BsonType.ARRAY, this.builderOutput)) {
      mergeBsonFromScope.writeLine("reader.readStartArray();");
      try (var loopScope =
          mergeBsonFromScope.openScope(
              "while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT)")) {
        loopScope.writeLine("addValueBuilder().mergeBsonFrom(reader);");
      }
      mergeBsonFromScope.writeLine("reader.readEndArray();");
      mergeBsonFromScope.writeLine("return this;");
    }

    try (var mergeBsonValue =
        CodeGeneratorUtils.openBsonValueMergeScope(
            this.descriptor, BsonType.ARRAY, this.builderOutput, this.messageOutput)) {
      try (var loopScope = mergeBsonValue.openScope("for (var elem : value)")) {
        loopScope.writeLine("addValueBuilder().mergeBsonFrom(elem);");
      }
      mergeBsonValue.writeLine("return this;");
    }

    return CodeGeneratorUtils.valueMessageFiles(
        this.descriptor, this.messageOutput, this.builderOutput);
  }
}
