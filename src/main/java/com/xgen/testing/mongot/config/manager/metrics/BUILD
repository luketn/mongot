load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "metrics",
    testonly = True,
    srcs = [
        "GroupedIndexGenerationMetricsBuilder.java",
        "IndexGenerationStateMetricsBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/config/manager/metrics"],
    deps = [
        "//src/main/java/com/xgen/mongot/config/manager/metrics",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_mongodb_bson",
    ],
)
