load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "expressions",
    srcs = [
        "AddExpressionTest.java",
        "ConstantExpressionTest.java",
        "GaussianDecayExpressionTest.java",
        "Log1PExpressionTest.java",
        "LogExpressionTest.java",
        "MultiplyExpressionTest.java",
        "PathExpressionTest.java",
        "ScoreExpressionTest.java",
    ],
    data = ["//src/test/unit/resources/index/query/scores/expressions:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/scores/expressions",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/query/scores/expressions",
        "@maven//:junit_junit",
    ],
)
