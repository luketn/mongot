load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "fakes",
    testonly = True,
    srcs = ["FakeStoredFields.java"],
    visibility = ["//visibility:public"],
    deps = ["@maven//:org_apache_lucene_lucene_core"],
)
