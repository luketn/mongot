load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "definition",
    testonly = True,
    srcs = [
        "CustomAnalyzerDefinitionBuilder.java",
        "OverriddenBaseAnalyzerDefinitionBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/analyzer/definition"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/analyzer/custom",
        "//src/main/java/com/xgen/mongot/index/analyzer/definition",
        "//src/main/java/com/xgen/mongot/util",
    ],
)
