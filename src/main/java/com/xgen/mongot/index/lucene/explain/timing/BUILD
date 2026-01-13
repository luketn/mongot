load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "timing",
    srcs = [
        "ExplainTimingBreakdown.java",
        "ExplainTimings.java",
        "QueryExecutionArea.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/util/timers",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/mongot/util/timers",
        "@maven//:com_google_guava_guava",
        "@maven//:org_mongodb_bson",
    ],
)
