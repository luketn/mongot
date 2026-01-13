load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "micrometer",
    testonly = True,
    srcs = [
        "PercentilesBuilder.java",
        "SerializableDistributionSummaryBuilder.java",
        "SerializableTimerBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/metrics/micrometer"],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics/micrometer",
    ],
)
