load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "counts",
    srcs = ["Count.java"],
    visibility = ["//:__subpackages__"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_mongodb_bson",
    ],
)
