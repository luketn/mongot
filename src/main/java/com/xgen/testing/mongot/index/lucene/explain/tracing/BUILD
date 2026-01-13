load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "tracing",
    testonly = True,
    srcs = ["FakeExplain.java"],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "@maven//:io_opentelemetry_opentelemetry_context",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/tracing",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:io_opentelemetry_opentelemetry_context",
    ],
)
