load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "catalog",
    srcs = [
        "DefaultIndexCatalog.java",
        "IndexCatalog.java",
        "InitializedIndexCatalog.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index"],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_guava_guava",
        "@maven//:org_mongodb_bson",
    ],
)
