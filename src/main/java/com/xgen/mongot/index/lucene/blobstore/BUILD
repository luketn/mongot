load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "blobstore",
    srcs = [
        "LuceneIndexSnapshotter.java",
        "LuceneIndexSnapshotterManager.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_apache_lucene_lucene_core"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/blobstore",
        "//src/main/java/com/xgen/mongot/index/version",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
