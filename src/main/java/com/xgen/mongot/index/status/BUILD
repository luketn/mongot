load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "status",
    srcs = [
        "IndexStatus.java",
        "RecoveringStatusReason.java",
        "StaleStatusReason.java",
        "StatusReason.java",
        "SynonymStatus.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:com_google_guava_guava",
        "@maven//:org_mongodb_bson",
    ],
)
