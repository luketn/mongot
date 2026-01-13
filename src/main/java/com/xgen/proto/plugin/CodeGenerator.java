package com.xgen.proto.plugin;

import static com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse;

public interface CodeGenerator {
  Iterable<CodeGeneratorResponse.File> generate();
}
