load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "handlers",
    srcs = ["LuceneDocumentUtilTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/ingestion/handlers",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
