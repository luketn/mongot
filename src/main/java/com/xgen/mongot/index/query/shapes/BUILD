load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "shapes",
    srcs = [
        "Box.java",
        "Circle.java",
        "GeometryShape.java",
        "Shape.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/mongot/util/geo",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
