load("@rules_cc//cc:repositories.bzl", "rules_cc_dependencies", "rules_cc_toolchains")
load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

def common_bazel_deps():
    rules_cc_dependencies()
    rules_cc_toolchains()

    rules_pkg_dependencies()
