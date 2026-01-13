load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "cursor",
    testonly = True,
    srcs = ["MongotCursorBatches.java"],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/cursor/serialization"],
    deps = [
        "//src/main/java/com/xgen/mongot/cursor",
        "//src/main/java/com/xgen/mongot/cursor/serialization",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "//src/main/java/com/xgen/testing/mongot/index",
        "//src/main/java/com/xgen/testing/mongot/mock/index",
        "@maven//:org_mongodb_bson",
    ],
)
