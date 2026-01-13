load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "executors",
    srcs = [
        "BulkheadCommandExecutorTest.java",
        "ExecutorManagerTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/server/command",
        "//src/main/java/com/xgen/mongot/server/executors",
        "//src/main/java/com/xgen/mongot/server/util",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:io_netty_netty_transport",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
