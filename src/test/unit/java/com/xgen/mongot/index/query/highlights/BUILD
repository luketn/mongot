load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "highlights",
    srcs = [
        "HighlightTest.java",
        "UnresolvedHighlightTest.java",
    ],
    data = ["//src/test/unit/resources/index/query/highlights:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/query/highlights",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/path/string",
        "//src/main/java/com/xgen/testing/mongot/index/query/highlights",
        "@maven//:junit_junit",
    ],
)
