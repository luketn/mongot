load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "common",
    srcs = [
        "ExplainSortField.java",
        "FieldComparatorExplainWrapper.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:org_apache_lucene_lucene_core",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/explainers",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/profiler",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/timing",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_jetbrains_annotations",
    ],
)
