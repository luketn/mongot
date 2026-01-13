load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "batch",
    srcs = [
        "AdjustableBatchSizeStrategyTest.java",
        "BatchSizeStrategySelectorTest.java",
        "ConstantBatchSizeStrategyTest.java",
        "ExponentiallyIncreasingBatchSizeStrategyTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/cursor/batch",
        "//src/main/java/com/xgen/testing/mongot/cursor/batch",
        "//src/main/java/com/xgen/testing/mongot/index/query",
        "//src/main/java/com/xgen/testing/mongot/index/query/operators",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:junit_junit",
    ],
)
