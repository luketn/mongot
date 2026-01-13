load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "util",
    srcs = [
        "BooleanComposerTest.java",
        "LucenePathTest.java",
        "RescoringTest.java",
        "SafeOperatorQueryBuilderTest.java",
        "SafeTermAutomatonQueryWrapperTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/lucene/query/util",
        "//src/main/java/com/xgen/mongot/index/path/string",
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/path/string",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_expressions",
        "@maven//:org_apache_lucene_lucene_queries",
        "@maven//:org_apache_lucene_lucene_sandbox",
        "@maven//:org_mockito_mockito_core",
    ],
)
