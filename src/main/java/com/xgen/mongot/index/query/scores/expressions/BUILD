load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "expressions",
    srcs = [
        "AddExpression.java",
        "ConstantExpression.java",
        "Expression.java",
        "GaussianDecayExpression.java",
        "Log1PExpression.java",
        "LogExpression.java",
        "MultiplyExpression.java",
        "PathExpression.java",
        "ScoreExpression.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:com_google_guava_guava",
        "@maven//:org_mongodb_bson",
    ],
)
