load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "similarity",
    srcs = [
        "LuceneSimilarity.java",
        "SimilarityWrapper.java",
        "SimilarityWrapperBuilder.java",
        "StableTflSimilarity.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:org_apache_lucene_lucene_core",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_flogger_flogger",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
