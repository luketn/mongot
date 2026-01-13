load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "scores",
    srcs = [
        "ConstantScore.java",
        "DismaxScore.java",
        "EmbeddedScore.java",
        "FunctionScore.java",
        "PathBoostScore.java",
        "Score.java",
        "ValueBoostScore.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/scores/expressions",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_mongodb_bson",
    ],
)
