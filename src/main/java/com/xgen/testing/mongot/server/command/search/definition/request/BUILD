load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "request",
    testonly = True,
    srcs = [
        "BatchOptionsDefinitionBuilder.java",
        "CursorOptionsDefinitionBuilder.java",
        "ExplainDefinitionBuilder.java",
        "GetMoreCommandDefinitionBuilder.java",
        "OptimizationFlagsDefinitionBuilder.java",
        "PlanShardedSearchCommandDefinitionBuilder.java",
        "SearchCommandDefinitionBuilder.java",
        "VectorSearchCommandDefinitionBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/server/command/search/definition/request"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/tracing",
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/mongot/server/command/search/definition/request",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_mongodb_bson",
    ],
)
