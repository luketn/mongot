load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "pushdown",
    srcs = ["MqlFieldFactoryTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/ingestion/pushdown",
        "//src/main/java/com/xgen/mongot/index/lucene/util",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing",
        "@maven//:com_google_guava_guava",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
)
