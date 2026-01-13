load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "version",
    testonly = True,
    srcs = ["GenerationIdBuilder.java"],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/version"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/version",
        "@maven//:org_mongodb_bson",
    ],
)
