load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "shapes",
    testonly = True,
    srcs = ["ShapeBuilder.java"],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/query/shapes"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/shapes",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
