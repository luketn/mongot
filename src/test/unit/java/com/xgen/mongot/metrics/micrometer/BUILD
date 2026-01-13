load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "micrometer",
    srcs = [
        "PercentilesTest.java",
        "SerializableDistributionSummaryTest.java",
        "SerializableTimerTest.java",
    ],
    data = ["//src/test/unit/resources/metrics/micrometer:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics/micrometer",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/metrics/micrometer",
        "@maven//:junit_junit",
    ],
)
