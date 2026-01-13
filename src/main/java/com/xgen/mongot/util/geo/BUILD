load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "geo",
    srcs = [
        "GeoJsonParser.java",
        "GeometryFlattener.java",
        "GeometryLeafStrategy.java",
        "LeafGeometry.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
