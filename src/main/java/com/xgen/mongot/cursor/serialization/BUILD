load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "serialization",
    srcs = [
        "MongotCursorBatch.java",
        "MongotCursorResult.java",
        "MongotIntermediateCursorBatch.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_mongodb_bson",
    ],
)
