load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "points",
    srcs = [
        "BooleanPoint.java",
        "DatePoint.java",
        "DoublePoint.java",
        "GeoPoint.java",
        "LongPoint.java",
        "NumericPoint.java",
        "ObjectIdPoint.java",
        "Point.java",
        "StringPoint.java",
        "UuidPoint.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/util",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/mongot/util/geo",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
