load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "wrapper",
    srcs = [
        "DynamicMappingsTest.java",
        "IndexAnalyzerBuilderTest.java",
        "LuceneAnalyzerTest.java",
        "QueryAnalyzerWrapperTest.java",
        "SearchAnalyzerBuilderTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/analyzer",
        "//src/main/java/com/xgen/mongot/index/analyzer/definition",
        "//src/main/java/com/xgen/mongot/index/analyzer/wrapper",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/path/string",
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/testing/mongot/index/analyzer/definition",
        "//src/main/java/com/xgen/testing/mongot/index/definition",
        "//src/main/java/com/xgen/testing/mongot/index/path/string",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
