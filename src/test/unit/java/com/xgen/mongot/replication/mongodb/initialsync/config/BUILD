load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "config",
    srcs = ["InitialSyncConfigTest.java"],
    data = ["//src/test/unit/resources/replication/mongodb/initialsync/config:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/replication/mongodb/initialsync/config",
        "//src/main/java/com/xgen/testing",
        "@maven//:junit_junit",
    ],
)
