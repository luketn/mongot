load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "serialization",
    testonly = True,
    srcs = [
        "MongotCursorBatchBuilder.java",
        "MongotCursorResultBuilder.java",
        "MongotIntermediateCursorBatchBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/cursor/serialization"],
    deps = [
        "//src/main/java/com/xgen/mongot/cursor/serialization",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_mongodb_bson",
    ],
)
