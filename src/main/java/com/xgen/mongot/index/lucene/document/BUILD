load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "document",
    srcs = [
        "DefaultIndexingPolicy.java",
        "LuceneIndexingPolicy.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/lucene/document/builder",
        "//src/main/java/com/xgen/mongot/index/lucene/document/single",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/ingestion/stored",
        "//src/main/java/com/xgen/mongot/index/lucene/document/block",
        "//src/main/java/com/xgen/mongot/index/lucene/document/builder",
        "//src/main/java/com/xgen/mongot/index/lucene/document/single",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_facet",
        "@maven//:org_mongodb_bson",
    ],
)
