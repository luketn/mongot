load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "analyzer",
    testonly = True,
    srcs = [
        "CharFilterTestUtil.java",
        "TokenFilterTestUtil.java",
        "TokenizerTestUtil.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/analyzer/custom",
        "//src/main/java/com/xgen/mongot/index/lucene/analyzer",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
