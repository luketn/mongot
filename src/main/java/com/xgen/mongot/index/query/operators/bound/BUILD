load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "bound",
    srcs = [
        "BooleanRangeBound.java",
        "DateRangeBound.java",
        "NumericRangeBound.java",
        "ObjectIdRangeBound.java",
        "RangeBound.java",
        "StringRangeBound.java",
        "UuidRangeBound.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/query/operators/type"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/operators/type",
        "//src/main/java/com/xgen/mongot/index/query/points",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:com_google_guava_guava",
    ],
)
