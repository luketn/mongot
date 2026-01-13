load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "timing",
    srcs = [
        "ExplainTimingBreakdownTest.java",
        "ExplainTimingsTest.java",
        "QueryExecutionAreaTest.java",
    ],
    data = ["//src/test/unit/resources/index/lucene/explain/timing:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/timing",
        "//src/main/java/com/xgen/mongot/util/timers",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/lucene/explain/timing",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
    ],
)
