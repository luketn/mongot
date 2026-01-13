load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "config",
    srcs = ["IndexDefinitionConfigTest.java"],
    data = ["//src/test/unit/resources/index/definition/config:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition/config",
        "//src/main/java/com/xgen/testing",
        "@maven//:junit_junit",
    ],
)
