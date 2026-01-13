load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "analyzer",
    testonly = True,
    srcs = [
        "AnalyzerRegistryBuilder.java",
        "AnalyzerTestUtil.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/analyzer"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/analyzer",
        "//src/main/java/com/xgen/mongot/index/analyzer/definition",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
