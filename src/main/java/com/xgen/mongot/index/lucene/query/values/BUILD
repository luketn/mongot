load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "values",
    srcs = [
        "AddValuesSource.java",
        "GaussianDecayValuesSource.java",
        "LogValuesSource.java",
        "MultiplyValuesSource.java",
        "PathValuesSource.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_apache_lucene_lucene_core"],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
