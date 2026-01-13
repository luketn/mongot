load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "profiler",
    srcs = [
        "ProfileDocIdSetIteratorTest.java",
        "ProfileLeafCollectorTest.java",
        "ProfileScorerTest.java",
        "ProfileWeightTest.java",
        "QueryProfilerFeatureExplainerTest.java",
        "QueryProfilerTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/information",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/profiler",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/query",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/timing",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/tracing",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing/mongot/index/lucene/explain/query",
        "//src/main/java/com/xgen/testing/mongot/mock/index",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mockito_mockito_core",
    ],
)
