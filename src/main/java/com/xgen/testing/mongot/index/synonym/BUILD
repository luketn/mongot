load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "synonym",
    testonly = True,
    srcs = ["SynonymDocumentBuilder.java"],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/synonym"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/synonym",
        "@maven//:org_mongodb_bson",
    ],
)
