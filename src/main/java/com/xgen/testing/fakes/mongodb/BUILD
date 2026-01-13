load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "mongodb",
    testonly = True,
    srcs = [
        "MockMongoChangeStreamCursor.java",
        "MockMongoCursor.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
    deps = [
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
        "@maven//:org_mongodb_mongodb_driver_sync",
    ],
)
