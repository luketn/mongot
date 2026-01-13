load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "metrics",
    testonly = True,
    srcs = [
        "SimpleMetricsFactory.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics",
        "@maven//:io_micrometer_micrometer_core",
    ],
)
