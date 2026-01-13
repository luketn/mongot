load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "metrics",
    srcs = [
        "GroupedIndexGenerationMetricsTest.java",
        "IndexGenerationStateMetricsTest.java",
    ],
    data = ["//src/test/unit/resources/config/manager/metrics:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/config/manager/metrics",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/config/manager/metrics",
        "//src/main/java/com/xgen/testing/mongot/index",
        "//src/main/java/com/xgen/testing/mongot/index/version",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
