package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;

import org.bson.BsonType;

public class ProtoDocumentCodeGenerator implements CodeGenerator {
  private final Descriptor descriptor;
  private final CodeOutput messageOutput = new CodeOutput();
  private final CodeOutput builderOutput = new CodeOutput();

  public ProtoDocumentCodeGenerator(Descriptor protoDocumentDescriptor) {
    this.descriptor = protoDocumentDescriptor;
  }

  @Override
  public Iterable<CodeGeneratorResponse.File> generate() {
    try (var writeBsonToScope = CodeGeneratorUtils.openWriteBsonToScope(this.messageOutput)) {
      writeBsonToScope.writeLine("writer.writeStartDocument();");
      try (var fieldScope = writeBsonToScope.openScope("for (var field : getFieldList())")) {
        fieldScope.writeLine("writer.writeName(field.getName());");
        fieldScope.writeLine("field.getValue().writeBsonTo(writer);");
      }
      writeBsonToScope.writeLine("writer.writeEndDocument();");
    }

    try (var mergeBsonFromScope =
        CodeGeneratorUtils.openMergeBsonFromReaderValidatedType(
            this.descriptor, BsonType.DOCUMENT, this.builderOutput)) {
      mergeBsonFromScope.writeLine("reader.readStartDocument();");
      try (var loopScope =
          mergeBsonFromScope.openScope(
              "while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT)")) {
        loopScope.writeLine(
            "addFieldBuilder().setName(reader.readName()).getValueBuilder().mergeBsonFrom(reader);");
      }
      mergeBsonFromScope.writeLine("reader.readEndDocument();");
      mergeBsonFromScope.writeLine("return this;");
    }

    try (var addField =
        this.builderOutput.openScope(
            "public Builder addField(String name, %s value)",
            WellKnownTypes.VALUE.getMessageClassName())) {
      addField.writeLine("addFieldBuilder().setName(name).setValue(value);");
      addField.writeLine("return this;");
    }

    try (var addField =
        this.builderOutput.openScope(
            "public Builder addField(String name, %s.Builder builder)",
            WellKnownTypes.VALUE.getMessageClassName())) {
      addField.writeLine("return addField(name, builder.build());");
    }

    return CodeGeneratorUtils.messageFiles(this.descriptor, this.messageOutput, this.builderOutput);
  }
}
