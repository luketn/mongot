load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "embedding",
    srcs = [
        "EmbeddingRequestContext.java",
        "MongotMetadata.java",
        "VectorOrError.java",
    ],
    visibility = ["//:__subpackages__"],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "@maven//:org_mongodb_bson",
    ],
)
