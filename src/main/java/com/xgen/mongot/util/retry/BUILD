load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "retry",
    srcs = [
        "ActionRetry.java",
        "BackoffPolicy.java",
        "BackoffRetryExecutor.java",
        "ExponentialBackoffPolicy.java",
        "FixedBackoffPolicy.java",
        "Retry.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/functionalinterfaces",
        "@maven//:net_jodah_failsafe",
    ],
)
