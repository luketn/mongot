load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "version",
    srcs = [
        "Generation.java",
        "GenerationId.java",
        "IndexCapabilities.java",
        "IndexFormatVersion.java",
        "MaterializedViewGeneration.java",
        "MaterializedViewGenerationId.java",
        "SynonymMappingId.java",
        "UserIndexVersion.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:org_mongodb_bson",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
