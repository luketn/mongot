load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "ingestion",
    srcs = ["LuceneIndexedFieldSpecTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/ingestion",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/ingestion/serialization",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
)
