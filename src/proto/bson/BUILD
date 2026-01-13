load("@rules_buf//buf:defs.bzl", "buf_lint_test")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/java:proto_bson.bzl", "java_bson_proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "types_proto",
    srcs = ["types.proto"],
    deps = ["//src/main/java/com/xgen/proto:options_proto"],
)

buf_lint_test(
    name = "types_proto_lint",
    config = ":buf.yaml",
    tags = ["lint"],
    targets = [":types_proto"],
)

proto_library(
    name = "sequence_token_proto",
    srcs = ["sequence-token.proto"],
    deps = ["types_proto"],
)

buf_lint_test(
    name = "sequence_token_proto_lint",
    config = ":buf.yaml",
    tags = ["lint"],
    targets = [":sequence_token_proto"],
)

java_bson_proto_library(
    name = "types_java_proto",
    srcs = [
        ":sequence_token_proto",
        ":types_proto",
    ],
    deps = ["//src/main/java/com/xgen/proto:options_java_proto"],
)
