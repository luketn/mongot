load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "explain",
    srcs = [
        "ProfilingIndexSearcher.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_apache_lucene_lucene_core"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/explainers",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/profiler",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/query",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/timing",
        "//src/main/java/com/xgen/mongot/index/lucene/query/custom",
        "//src/main/java/com/xgen/mongot/index/lucene/searcher",
        "//src/main/java/com/xgen/mongot/trace",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
