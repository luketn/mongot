load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "autoembedding",
    srcs = ["MaterializedViewGenerationIdTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/autoembedding",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/util/functionalinterfaces",
        "//src/main/java/com/xgen/testing/mongot/index/version",
        "@maven//:com_google_guava_guava",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
