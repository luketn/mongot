load("@contrib_rules_jvm//java:defs.bzl", "checkstyle_binary", "checkstyle_config")
load("@rules_jvm_external//:defs.bzl", "artifact")

filegroup(
    name = "checkstyle",
    srcs = ["mongot.xml"],
    visibility = ["//visibility:public"],
)

checkstyle_binary(
    name = "checkstyle_cli",
    testonly = True,
    runtime_deps = [
        artifact("com.puppycrawl.tools:checkstyle"),
    ],
)

checkstyle_config(
    name = "mongot",
    testonly = True,
    checkstyle_binary = ":checkstyle_cli",
    config_file = ":mongot.xml",
    data = ["checkstyle-suppressions.xml"],
    visibility = ["//visibility:public"],
)
