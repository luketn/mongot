load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "scores",
    testonly = True,
    srcs = [
        "ConstantScoreBuilder.java",
        "DismaxScoreBuilder.java",
        "EmbeddedScoreBuilder.java",
        "FunctionScoreBuilder.java",
        "PathBoostScoreBuilder.java",
        "ScoreBuilder.java",
        "ValueBoostScoreBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/query/scores"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/scores",
        "//src/main/java/com/xgen/mongot/index/query/scores/expressions",
        "//src/main/java/com/xgen/mongot/util",
    ],
)
