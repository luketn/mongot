load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "mongod",
    srcs = [
        "ConfigUpdaterUtils.java",
        "MongodConnectionString.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/mongot/util/mongodb",
        "//src/main/java/com/xgen/mongot/util/mongodb/serialization",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_mongodb_bson",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
