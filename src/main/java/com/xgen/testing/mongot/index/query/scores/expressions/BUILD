load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "expressions",
    testonly = True,
    srcs = [
        "AddExpressionBuilder.java",
        "ConstantExpressionBuilder.java",
        "GaussianDecayExpressionBuilder.java",
        "Log1PExpressionBuilder.java",
        "LogExpressionBuilder.java",
        "MultiplyExpressionBuilder.java",
        "PathExpressionBuilder.java",
        "ScoreExpressionBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/index/query/scores/expressions"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/scores/expressions",
        "//src/main/java/com/xgen/mongot/util",
    ],
)
