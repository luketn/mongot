load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "query",
    testonly = True,
    srcs = [
        "MockQueryChildren.java",
        "MockQueryExecutionContextNode.java",
        "RewrittenChildClausesBuilder.java",
        "RewrittenQueryExecutionContextNodeBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/query",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/timing",
        "@maven//:org_apache_lucene_lucene_core",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/query",
        "//src/main/java/com/xgen/mongot/index/lucene/explain/timing",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
