load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "handlers",
    srcs = [
        "Composite.java",
        "DocumentHandler.java",
        "FieldValueHandler.java",
        "LuceneDocumentUtil.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_apache_lucene_lucene_core"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/util/bson",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
