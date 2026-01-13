load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "mql",
    srcs = [
        "ClauseTest.java",
        "ClauseTests.java",
        "ComparisonOperatorTest.java",
    ],
    data = ["//src/test/unit/resources/index/query/operators/mql:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/operators/mql",
        "//src/main/java/com/xgen/mongot/index/query/operators/value",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/query/operators/mql",
        "@maven//:junit_junit",
    ],
)
