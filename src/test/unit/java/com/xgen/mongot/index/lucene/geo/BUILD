load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "geo",
    srcs = [
        "LuceneGeoTranslatorTest.java",
        "LuceneGeometryFieldsTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/geo",
        "//src/main/java/com/xgen/mongot/util/geo",
        "//src/main/java/com/xgen/testing/mongot/mock/index",
        "//src/main/java/com/xgen/testing/mongot/util/geo",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
