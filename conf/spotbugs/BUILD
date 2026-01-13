load("@contrib_rules_jvm//java:defs.bzl", "spotbugs_binary", "spotbugs_config")
load("@rules_jvm_external//:defs.bzl", "artifact")

filegroup(
    name = "spotbugs",
    srcs = ["spotbugs-exclude.xml"],
    visibility = ["//visibility:public"],
)

spotbugs_binary(
    name = "spotbugs_cli",
    testonly = True,
    runtime_deps = [
        artifact("com.github.spotbugs:spotbugs"),
    ],
)

spotbugs_config(
    name = "mongot",
    testonly = True,
    effort = "default",
    exclude_filter = ":spotbugs",
    fail_on_warning = True,
    spotbugs_binary = ":spotbugs_cli",
    visibility = ["//visibility:public"],
)
