load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "micrometer",
    srcs = [
        "Percentiles.java",
        "SerializableDistributionSummary.java",
        "SerializableTimer.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_mongodb_bson",
    ],
)
