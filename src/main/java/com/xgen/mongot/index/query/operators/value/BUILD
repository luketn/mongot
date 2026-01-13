load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "value",
    srcs = [
        "BooleanValue.java",
        "DateValue.java",
        "NonNullValue.java",
        "NullValue.java",
        "NumericValue.java",
        "ObjectIdValue.java",
        "StringValue.java",
        "UuidValue.java",
        "Value.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/query/operators/bound",
        "//src/main/java/com/xgen/mongot/index/query/operators/type",
        "//src/main/java/com/xgen/mongot/index/query/points",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/operators/bound",
        "//src/main/java/com/xgen/mongot/index/query/operators/type",
        "//src/main/java/com/xgen/mongot/index/query/points",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_mongodb_bson",
    ],
)
