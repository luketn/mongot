load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "points",
    srcs = ["PointTest.java"],
    data = ["//src/test/unit/resources/index/query/points:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/points",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/util/geo",
        "@maven//:junit_junit",
    ],
)
