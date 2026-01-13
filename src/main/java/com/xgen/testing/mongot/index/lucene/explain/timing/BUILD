load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "timing",
    testonly = True,
    srcs = [
        "ExplainTimingBreakdownBuilder.java",
        "QueryExecutionAreaBuilder.java",
        "TimingTestUtil.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/lucene/explain/timing"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/timing",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/timers",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_truth_truth",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_subclass",  # keep
    ],
)
