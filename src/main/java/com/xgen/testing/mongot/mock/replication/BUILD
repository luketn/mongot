load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "replication",
    testonly = True,
    srcs = ["ReplicationManager.java"],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/replication"],
    deps = [
        "//src/main/java/com/xgen/mongot/replication",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_mockito_mockito_core",
    ],
)
