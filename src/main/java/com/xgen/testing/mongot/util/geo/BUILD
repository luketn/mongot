load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "geo",
    testonly = True,
    srcs = ["MockGeometries.java"],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
