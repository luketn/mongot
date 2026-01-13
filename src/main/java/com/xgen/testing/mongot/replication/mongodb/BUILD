load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "mongodb",
    testonly = True,
    srcs = ["ChangeStreamUtils.java"],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/replication/mongodb/common",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:commons_codec_commons_codec",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
