load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "query",
    testonly = True,
    srcs = ["Query.java"],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/testing/mongot/index/query",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/testing/mongot/index/query",
        "//src/main/java/com/xgen/testing/mongot/index/query/operators",
    ],
)
