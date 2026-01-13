load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "pushdown",
    srcs = [
        "ArrayComparator.java",
        "MqlComparator.java",
        "MqlDouble.java",
        "MqlNumberUtils.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/query/sort/mixed",
        "@maven//:org_mongodb_bson",
    ],
)
