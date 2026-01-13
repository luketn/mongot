load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "variations",
    testonly = True,
    srcs = [
        "ClusterConfiguration.java",
        "ShardZoneConfigName.java",
        "SyncMode.java",
        "TestRunVariant.java",
        "TestType.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_mongodb_bson",
    ],
)
