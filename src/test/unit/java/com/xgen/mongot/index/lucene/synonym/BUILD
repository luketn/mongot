load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "synonym",
    srcs = [
        "LuceneSynonymMapBuilderTest.java",
        "LuceneSynonymRegistryTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/analyzer",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/synonym",
        "//src/main/java/com/xgen/mongot/index/status",
        "//src/main/java/com/xgen/mongot/index/synonym",
        "//src/main/java/com/xgen/mongot/util/functionalinterfaces",
        "//src/main/java/com/xgen/testing/mongot/index/analyzer",
        "//src/main/java/com/xgen/testing/mongot/index/definition",
        "@maven//:com_pholser_junit_quickcheck_core",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_analysis_common",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
