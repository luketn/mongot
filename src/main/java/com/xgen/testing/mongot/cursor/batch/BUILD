load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "batch",
    testonly = True,
    srcs = [
        "BatchCursorOptionsBuilder.java",
        "QueryCursorOptionsBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/cursor/batch"],
    deps = ["//src/main/java/com/xgen/mongot/cursor/batch"],
)
