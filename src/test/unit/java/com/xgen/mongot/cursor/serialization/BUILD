load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "serialization",
    srcs = [
        "MongotCursorBatchesTest.java",
        "MongotCursorResultTest.java",
        "MongotIntermediateCursorBatchTest.java",
    ],
    data = ["//src/test/unit/resources/cursor/serialization:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/cursor/serialization",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/cursor/serialization",
        "//src/main/java/com/xgen/testing/mongot/index",
        "//src/main/java/com/xgen/testing/mongot/index/lucene/explain/information",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
