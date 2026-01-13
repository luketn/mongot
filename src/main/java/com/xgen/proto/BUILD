load("@rules_buf//buf:defs.bzl", "buf_lint_test")
load("@rules_java//java:defs.bzl", "java_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/java:package.bzl", "java_library")

# TODO: remove ignores and exclusions to fully adopt gazelle.
# gazelle:exclude plugin

java_library(
    name = "proto",
    srcs = [
        "BsonMessage.java",
        "BsonProtoParseException.java",
        "BsonValueMessage.java",
        "DiscriminatedUnionFieldFilteringBsonReader.java",
        "DiscriminatedUnionVariantBsonWriter.java",
        "NumericUtils.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "@com_google_protobuf//:protobuf_java",
        "@maven//:org_mongodb_bson",
    ],
)

proto_library(
    name = "options_proto",
    srcs = ["options.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_protobuf//:descriptor_proto",
    ],
)

buf_lint_test(
    name = "options_proto_proto_lint",
    config = ":buf.yaml",
    tags = ["lint"],
    targets = [":options_proto"],
)

java_proto_library(
    name = "options_java_proto",
    visibility = ["//visibility:public"],
    deps = [":options_proto"],
)
