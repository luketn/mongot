load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "scores",
    srcs = [
        "ConstantScoreTest.java",
        "DismaxScoreTest.java",
        "EmbeddedScoreTest.java",
        "FunctionScoreTest.java",
        "PathBoostScoreTest.java",
        "ScoreTest.java",
        "ScoreTests.java",
        "ValueBoostScoreTest.java",
    ],
    data = ["//src/test/unit/resources/index/query/scores:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/scores",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/query/scores",
        "//src/main/java/com/xgen/testing/mongot/index/query/scores/expressions",
        "@maven//:junit_junit",
    ],
)
