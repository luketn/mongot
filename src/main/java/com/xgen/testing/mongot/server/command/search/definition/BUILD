load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "definition",
    testonly = True,
    srcs = [
        "ShardedSearchPlanBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/server/command/search/definition"],
    deps = [
        "//src/main/java/com/xgen/mongot/server/command/search/definition",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_mongodb_bson",
    ],
)
