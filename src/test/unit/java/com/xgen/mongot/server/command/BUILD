load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "command",
    srcs = ["WireCommandParserTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/server/command",
        "//src/main/java/com/xgen/mongot/server/message",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
