load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "definition",
    srcs = [
        "PlanShardedSearchCommandResponseDefinitionTest.java",
    ],
    data = ["//src/test/unit/resources/server/command/search/definition:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/server/command/search/definition",
        "//src/main/java/com/xgen/testing",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
