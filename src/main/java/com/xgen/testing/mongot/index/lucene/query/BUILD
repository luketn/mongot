load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "query",
    testonly = True,
    srcs = ["ScoreDetailsWrappedQueryBuilder.java"],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/lucene/query"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/query",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
