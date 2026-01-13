load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "builder",
    srcs = [
        "DocumentBlockBuilder.java",
        "DocumentBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_apache_lucene_lucene_core"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/ingestion/handlers",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
