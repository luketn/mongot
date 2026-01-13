load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "management",
    srcs = [
        "AbstractManageSearchIndexCommandFactory.java",
        "IndexManagementCommandContext.java",
        "ManageSearchIndexCommandFactory.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/server/command",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/server/command",
        "//src/main/java/com/xgen/mongot/server/command/management/definition",
        "//src/main/java/com/xgen/mongot/server/command/management/definition/common",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_mongodb_bson",
    ],
)
