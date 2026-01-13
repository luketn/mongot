load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "string",
    srcs = [
        "StringFieldPath.java",
        "StringMultiFieldPath.java",
        "StringPath.java",
        "UnresolvedStringFieldPath.java",
        "UnresolvedStringMultiFieldPath.java",
        "UnresolvedStringPath.java",
        "UnresolvedStringWildcardPath.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_mongodb_bson",
    ],
)
