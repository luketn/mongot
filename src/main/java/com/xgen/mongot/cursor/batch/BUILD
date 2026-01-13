load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "batch",
    srcs = [
        "AdjustableBatchSizeStrategy.java",
        "BatchCursorOptions.java",
        "BatchSizeStrategy.java",
        "BatchSizeStrategySelector.java",
        "ConstantBatchSizeStrategy.java",
        "ExponentiallyIncreasingBatchSizeStrategy.java",
        "QueryCursorOptions.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_guava_guava",
    ],
)
