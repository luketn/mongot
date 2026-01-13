load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "config",
    srcs = ["LuceneConfig.java"],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/config/util",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_lucene_lucene_backward_codecs",  # keep: required to read indices with legacy codecs
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
