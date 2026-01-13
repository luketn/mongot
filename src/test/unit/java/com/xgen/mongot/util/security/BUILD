load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "security",
    srcs = ["SecurityTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/security",
        "@maven//:junit_junit",
    ],
)
