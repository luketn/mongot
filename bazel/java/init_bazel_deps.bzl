load("@rules_java//java:repositories.bzl", "rules_java_toolchains")

def java_init_bazel_deps():
    rules_java_toolchains()
