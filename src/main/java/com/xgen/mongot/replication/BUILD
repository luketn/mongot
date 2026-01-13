load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "replication",
    srcs = [
        "InitialSyncStatus.java",
        "ReplicationManager.java",
        "ReplicationManagerFactory.java",
        "ReplicationStatus.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/util/mongodb"],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/util/mongodb",
    ],
)
