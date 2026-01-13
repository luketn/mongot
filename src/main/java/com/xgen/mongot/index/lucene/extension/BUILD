load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "extension",
    srcs = [
        "KnnByteVectorField.java",
        "KnnFieldType.java",
        "KnnFloatVectorField.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_apache_lucene_lucene_core"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
