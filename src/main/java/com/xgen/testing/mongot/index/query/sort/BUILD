load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "sort",
    testonly = True,
    srcs = [
        "MetaSortOptionsBuilder.java",
        "SortFieldBuilder.java",
        "SortOptionsBuilder.java",
        "SortSpecBuilder.java",
        "UserFieldSortOptionsBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/query/sort"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/sort",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_guava_guava",
    ],
)
