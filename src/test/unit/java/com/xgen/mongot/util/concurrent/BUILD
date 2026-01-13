load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "concurrent",
    srcs = [
        "ExecutorsTest.java",
        "FileLockGuardTest.java",
        "LockGuardTest.java",
        "MeteredCallerRunsPolicyTest.java",
        "OneShotSingleThreadExecutorTest.java",
        "RecurringTimerTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "//src/main/java/com/xgen/testing",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
    ],
)
