load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "definition",
    srcs = [
        "PlanShardedSearchCommandResponseDefinition.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_mongodb_bson",
    ],
)
