load("@maven//:compat.bzl", "compat_repositories")
load("@maven//:defs.bzl", "pinned_maven_install")

def java_init_deps():
    # Note: rules_java_dependencies() is called in early_init.bzl
    # Note: rules_java_toolchains() is called in init_bazel_deps.bzl

    # pinned_maven_install() is loaded from @maven, which is created by deps.bzl:java_dependencies().
    # Therefore we can't call load("@maven...) until after java_dependencies() is called.
    # Therefore, we must call this in a file loaded latter.
    pinned_maven_install()

    compat_repositories()
