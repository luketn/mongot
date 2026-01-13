load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "blobstore",
    srcs = [
        "BlobstoreSnapshotterManager.java",
        "IndexBlobstoreSnapshotter.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/blobstore",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/version",
    ],
)
