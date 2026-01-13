load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "parsers",
    srcs = [
        "GeometryParser.java",
        "KnnVectorParser.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/util/bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/mongot/util/geo",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
