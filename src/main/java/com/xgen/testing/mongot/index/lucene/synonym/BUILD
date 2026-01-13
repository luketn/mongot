load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "synonym",
    testonly = True,
    srcs = ["SynonymRegistryBuilder.java"],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/synonym"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/synonym",
        "//src/main/java/com/xgen/mongot/index/synonym",
        "//src/main/java/com/xgen/testing/mongot/index/analyzer",
    ],
)
