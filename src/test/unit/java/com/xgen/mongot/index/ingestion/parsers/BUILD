load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "parsers",
    srcs = ["KnnVectorParserTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/ingestion/parsers",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
