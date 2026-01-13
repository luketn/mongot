load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "highlights",
    testonly = True,
    srcs = [
        "HighlightBuilder.java",
        "UnresolvedHighlightBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/query/highlights"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/path/string",
        "//src/main/java/com/xgen/mongot/index/query/highlights",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing/mongot/index/path/string",
    ],
)
