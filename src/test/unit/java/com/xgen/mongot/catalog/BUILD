load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "catalog",
    srcs = [
        "DefaultIndexCatalogTest.java",
        "InitializedIndexCatalogTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/catalog",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/definition",
        "//src/main/java/com/xgen/testing/mongot/mock/index",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
