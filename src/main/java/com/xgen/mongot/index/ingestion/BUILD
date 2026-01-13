load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "ingestion",
    srcs = [
        "BsonDocumentProcessor.java",
        "ResettingLazySupplier.java",
        "SkippingLazySupplier.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/ingestion/handlers",
        "//src/main/java/com/xgen/mongot/index/ingestion/parsers",
        "//src/main/java/com/xgen/mongot/util/bson",
        "@maven//:com_google_flogger_flogger",
        "@maven//:org_mongodb_bson",
    ],
)
