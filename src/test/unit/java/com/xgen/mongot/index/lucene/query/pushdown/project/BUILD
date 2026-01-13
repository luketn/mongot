load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "project",
    srcs = [
        "ExclusiveProjectionTest.java",
        "IdLookupFactoryTest.java",
        "InclusiveBlobProjectTest.java",
        "PathTrieTest.java",
        "StoredSourceStrategyTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/lucene/query/pushdown/project",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/util",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
)
