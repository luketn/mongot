load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "command",
    srcs = [
        "Command.java",
        "CommandFactory.java",
        "CommandFactoryMarker.java",
        "ParsedCommand.java",
        "WireCommandParser.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/server/message",
        "//src/main/java/com/xgen/mongot/util",
        "//src/proto/searchenvoy:searchenvoy_java_proto",
        "@maven//:org_mongodb_bson",
    ],
)
