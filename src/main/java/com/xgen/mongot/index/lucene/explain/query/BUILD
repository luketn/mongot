load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "query",
    srcs = [
        "ChildClauses.java",
        "ExplainVisitor.java",
        "QueryChildren.java",
        "QueryExecutionContext.java",
        "QueryExecutionContextNode.java",
        "QueryVisitorQueryExecutionContext.java",
        "QueryVisitorQueryExecutionContextNode.java",
        "RewrittenChildClauses.java",
        "RewrittenQueryExecutionContextNode.java",
        "RewrittenQueryNodeException.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/timing",
        "@maven//:org_apache_lucene_lucene_core",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/explain/timing",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
