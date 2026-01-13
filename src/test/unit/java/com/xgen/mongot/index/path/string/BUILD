load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "string",
    srcs = [
        "StringFieldPathTest.java",
        "StringMultiFieldPathTest.java",
        "StringPathTest.java",
        "UnresolvedStringFieldPathTest.java",
        "UnresolvedStringMultiFieldPathTest.java",
        "UnresolvedStringPathTest.java",
        "UnresolvedStringWildcardPathTest.java",
    ],
    data = ["//src/test/unit/resources/index/path/string:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/path/string",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/path/string",
        "@maven//:junit_junit",
    ],
)
