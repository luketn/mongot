load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "metrics",
    srcs = [
        "GroupedIndexGenerationMetrics.java",
        "IndexConfigState.java",
        "IndexGenerationStateMetrics.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:com_google_guava_guava",
        "@maven//:org_mongodb_bson",
    ],
)
