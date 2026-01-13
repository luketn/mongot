load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "custom",
    testonly = True,
    srcs = [
        "CustomCharFilterDefinitionBuilder.java",
        "TokenFilterDefinitionBuilder.java",
        "TokenizerDefinitionBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/analyzer/custom"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/analyzer/custom",
        "//src/main/java/com/xgen/mongot/util",
    ],
)
