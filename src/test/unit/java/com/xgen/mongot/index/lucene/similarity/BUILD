load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "similarity",
    srcs = [
        "LuceneSimilarityTest.java",
        "StableTflSimilarityTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/similarity",
        "//src/main/java/com/xgen/testing/mongot/index/definition",
        "@maven//:com_google_flogger_flogger_system_backend",  # keep
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
