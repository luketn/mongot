load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "counts",
    srcs = ["CountTest.java"],
    data = ["//src/test/unit/resources/index/query/counts:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/counts",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/query/counts",
        "@maven//:junit_junit",
    ],
)
