load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "synonym",
    srcs = ["SynonymDocumentTest.java"],
    data = ["//src/test/unit/resources/index/synonym:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/synonym",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/synonym",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
