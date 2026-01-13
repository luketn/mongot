package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.EnumDescriptor;
import static com.google.protobuf.Descriptors.FileDescriptor;
import static com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Collects descriptors that code generation will run on. */
public class DescriptorCollector {
  private final List<Descriptor> messageDescriptors;
  private final List<EnumDescriptor> enumDescriptors;

  private DescriptorCollector(
      List<Descriptor> messageDescriptors, List<EnumDescriptor> enumDescriptors) {
    this.messageDescriptors = Collections.unmodifiableList(messageDescriptors);
    this.enumDescriptors = Collections.unmodifiableList(enumDescriptors);
  }

  public List<Descriptor> getMessageDescriptors() {
    return this.messageDescriptors;
  }

  public List<EnumDescriptor> getEnumDescriptors() {
    return this.enumDescriptors;
  }

  public static DescriptorCollector create(CodeGeneratorRequest request) throws Exception {
    var messageDescriptors = new ArrayList<Descriptor>();
    var enumDescriptors = new ArrayList<EnumDescriptor>();
    for (var fileDescriptor : getFileDescriptorsToGenerate(request)) {
      addDescriptors(
          fileDescriptor.getMessageTypes(),
          fileDescriptor.getEnumTypes(),
          messageDescriptors,
          enumDescriptors);
    }
    return new DescriptorCollector(messageDescriptors, enumDescriptors);
  }

  private static List<FileDescriptor> getFileDescriptorsToGenerate(CodeGeneratorRequest request)
      throws Exception {
    var toGenerate = new ArrayList<Descriptors.FileDescriptor>();
    var allFileDescriptors = new ArrayList<Descriptors.FileDescriptor>(request.getProtoFileCount());
    for (DescriptorProtos.FileDescriptorProto fileDescriptorProto : request.getProtoFileList()) {
      var fileDescriptor =
          FileDescriptor.buildFrom(
              fileDescriptorProto, allFileDescriptors.toArray(new FileDescriptor[0]));
      if (request.getFileToGenerateList().contains(fileDescriptorProto.getName())) {
        toGenerate.add(fileDescriptor);
      }
      allFileDescriptors.add(fileDescriptor);
    }
    return toGenerate;
  }

  private static void addDescriptors(
      List<Descriptor> inputMessageDescriptors,
      List<EnumDescriptor> inputEnumDescriptors,
      List<Descriptor> outputMessageDescriptors,
      List<EnumDescriptor> outputEnumDescriptors) {
    outputEnumDescriptors.addAll(inputEnumDescriptors);
    for (var inputMessageDescriptor : inputMessageDescriptors) {
      // NB: map entries are a hidden parts of the map API and do not have insertion points.
      if (inputMessageDescriptor.getOptions().getMapEntry()) {
        continue;
      }
      outputMessageDescriptors.add(inputMessageDescriptor);
      addDescriptors(
          inputMessageDescriptor.getNestedTypes(),
          inputMessageDescriptor.getEnumTypes(),
          outputMessageDescriptors,
          outputEnumDescriptors);
    }
  }
}
