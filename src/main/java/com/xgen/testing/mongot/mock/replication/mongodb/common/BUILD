load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "common",
    testonly = True,
    srcs = [
        "DocumentIndexer.java",
        "SessionRefresher.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/replication/mongodb/common"],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/replication/mongodb/common",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
