package com.xgen.proto.plugin;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.EnumDescriptor;
import static com.google.protobuf.Descriptors.FileDescriptor;

import com.google.protobuf.Descriptors;
import java.util.Optional;

public class NameUtils {
  private NameUtils() {}

  public static String getJavaPackagePath(FileDescriptor fileDescriptor) {
    var fileOptions = fileDescriptor.getOptions();
    return fileOptions.getJavaPackage().isEmpty()
        ? fileDescriptor.getPackage()
        : fileOptions.getJavaPackage();
  }

  public static String getJavaFilename(Descriptor messageDescriptor) {
    var basename = getBasename(messageDescriptor.getFile());
    while (basename.isEmpty()) {
      if (messageDescriptor.getContainingType() == null) {
        basename = Optional.of(messageDescriptor.getName());
      } else {
        messageDescriptor = messageDescriptor.getContainingType();
      }
    }
    return getJavaFilename(messageDescriptor.getFile(), basename.get());
  }

  public static String getJavaFilename(EnumDescriptor enumDescriptor) {
    var basename = getBasename(enumDescriptor.getFile());
    if (basename.isEmpty() && enumDescriptor.getContainingType() == null) {
      return getJavaFilename(enumDescriptor.getFile(), enumDescriptor.getName());
    }
    Descriptors.Descriptor messageDescriptor = enumDescriptor.getContainingType();
    while (basename.isEmpty()) {
      if (messageDescriptor.getContainingType() == null) {
        basename = Optional.of(messageDescriptor.getName());
      } else {
        messageDescriptor = messageDescriptor.getContainingType();
      }
    }
    return getJavaFilename(messageDescriptor.getFile(), basename.get());
  }

  private static String getJavaFilename(FileDescriptor fileDescriptor, String basename) {
    return getJavaPackagePath(fileDescriptor).replace(".", "/") + "/" + basename + ".java";
  }

  private static Optional<String> getBasename(FileDescriptor fileDescriptor) {
    var fileOptions = fileDescriptor.getOptions();
    return fileOptions.getJavaMultipleFiles()
        ? Optional.empty()
        : Optional.of(fileOptions.getJavaOuterClassname());
  }
}
