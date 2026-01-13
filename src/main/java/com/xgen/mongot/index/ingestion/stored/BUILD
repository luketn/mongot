load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "stored",
    srcs = [
        "StoredBuilder.java",
        "StoredDocumentBuilder.java",
        "StoredDocumentHandler.java",
        "StoredFieldValueHandler.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/ingestion/handlers",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/ingestion/handlers",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
