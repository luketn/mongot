load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "wrapper",
    srcs = [
        "AnalyzerWrapperBuilder.java",
        "DynamicTypeSetPrefixMap.java",
        "IndexAnalyzerBuilder.java",
        "IndexAnalyzerWrapper.java",
        "LuceneAnalyzer.java",
        "QueryAnalyzerBuilder.java",
        "QueryAnalyzerWrapper.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/analyzer",
        "@maven//:org_apache_lucene_lucene_core",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/analyzer",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/path/string",
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
