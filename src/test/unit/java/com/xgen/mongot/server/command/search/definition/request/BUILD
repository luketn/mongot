load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "request",
    srcs = [
        "BatchOptionsDefinitionTest.java",
        "CursorOptionsDefinitionTest.java",
        "ExplainDefinitionTest.java",
        "GetMoreCommandDefinitionTest.java",
        "OptimizationFlagsDefinitionTest.java",
        "PlanShardedSearchDefinitionTest.java",
        "SearchCommandDefinitionTest.java",
        "VectorSearchCommandDefinitionTest.java",
    ],
    data = ["//src/test/unit/resources/server/command/search/definition/request:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/tracing",
        "//src/main/java/com/xgen/mongot/index/query/operators",
        "//src/main/java/com/xgen/mongot/index/query/operators/mql",
        "//src/main/java/com/xgen/mongot/server/command/search/definition/request",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/query",
        "//src/main/java/com/xgen/testing/mongot/index/query/operators/mql",
        "//src/main/java/com/xgen/testing/mongot/server/command/search/definition/request",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
