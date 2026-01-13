load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "pushdown",
    srcs = [
        "FallbackMarker.java",
        "MqlField.java",
        "MqlFieldFactory.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/util",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_mongodb_bson",
    ],
)
