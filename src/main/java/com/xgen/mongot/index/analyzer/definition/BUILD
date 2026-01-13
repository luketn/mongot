load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "definition",
    srcs = [
        "AnalyzerDefinition.java",
        "CustomAnalyzerDefinition.java",
        "NormalizerDefinition.java",
        "OverriddenBaseAnalyzerDefinition.java",
        "StockAnalyzerNames.java",
        "StockNormalizerName.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/analyzer/attributes",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/analyzer/attributes",
        "//src/main/java/com/xgen/mongot/index/analyzer/custom",
        "//src/main/java/com/xgen/mongot/index/lucene/analyzer",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
)
