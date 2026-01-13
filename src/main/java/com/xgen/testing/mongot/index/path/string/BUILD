load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "string",
    testonly = True,
    srcs = [
        "StringPathBuilder.java",
        "UnresolvedStringPathBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/path/string"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/path/string",
        "//src/main/java/com/xgen/mongot/util",
    ],
)
