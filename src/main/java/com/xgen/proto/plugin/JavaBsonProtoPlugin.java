package com.xgen.proto.plugin;

import static com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest;
import static com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;

import com.google.protobuf.ExtensionRegistry;
import org.bson.protobuf.MessageType;
import org.bson.protobuf.Options;

public class JavaBsonProtoPlugin {
  private static void validateRequest(CodeGeneratorRequest request) {
    for (FileDescriptorProto fileDescriptor : request.getProtoFileList()) {
      if (fileDescriptor.getOptions().getJavaOuterClassname().isEmpty()
          && !fileDescriptor.getOptions().getJavaMultipleFiles()) {
        throw new IllegalArgumentException(
            String.format(
                "%s must set either java_outer_classname or java_multiple_files",
                fileDescriptor.getName()));
      }
    }
  }

  /**
   * Return a CodeGenerator for a particular message. This allows us to override behavior for well
   * known types messages which directly map to BSON types.
   *
   * @param messageDescriptor to generate code for
   * @return an appropriate CodeGenerator
   */
  static CodeGenerator generatorForMessage(Descriptor messageDescriptor) {
    return WellKnownTypes.get(messageDescriptor)
        .map(wkt -> wkt.newCodeGenerator(messageDescriptor))
        .orElseGet(() -> genericCodeGenerator(messageDescriptor));
  }

  private static CodeGenerator genericCodeGenerator(Descriptor messageDescriptor) {
    if (CodeGeneratorUtils.getMessageType(messageDescriptor) == MessageType.VALUE) {
      return new GenericValueMessageCodeGenerator(messageDescriptor);
    } else {
      return new GenericMessageCodeGenerator(messageDescriptor);
    }
  }

  public static void main(String[] args) throws Exception {
    var registry = ExtensionRegistry.newInstance();
    registry.add(Options.fieldOptions);
    registry.add(Options.messageOptions);
    CodeGeneratorRequest request = CodeGeneratorRequest.parseFrom(System.in, registry);
    validateRequest(request);
    DescriptorCollector toGenerate = DescriptorCollector.create(request);
    CodeGeneratorResponse.Builder response = CodeGeneratorResponse.newBuilder();
    response.setSupportedFeatures(CodeGeneratorResponse.Feature.FEATURE_PROTO3_OPTIONAL_VALUE);
    for (var messageDescriptor : toGenerate.getMessageDescriptors()) {
      response.addAllFile(generatorForMessage(messageDescriptor).generate());
    }
    for (var enumDescriptor : toGenerate.getEnumDescriptors()) {
      response.addAllFile(new EnumCodeGenerator(enumDescriptor).generate());
    }
    response.build().writeTo(System.out);
  }
}
