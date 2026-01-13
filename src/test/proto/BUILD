load("@rules_buf//buf:defs.bzl", "buf_lint_test")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/java:proto_bson.bzl", "java_bson_proto_library")

proto_library(
    name = "proto3_proto",
    srcs = ["proto3.proto"],
    deps = [
        "//src/main/java/com/xgen/proto:options_proto",
        "//src/proto/bson:types_proto",
    ],
)

buf_lint_test(
    name = "proto3_proto_lint",
    config = ":buf.yaml",
    tags = ["lint"],
    targets = [":proto3_proto"],
)

java_bson_proto_library(
    name = "proto3_java_proto",
    testonly = True,
    srcs = [":proto3_proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/proto:options_java_proto",
        "//src/proto/bson:types_java_proto",
    ],
)
