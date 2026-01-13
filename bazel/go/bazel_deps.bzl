load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")
load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

def go_bazel_deps():
    go_rules_dependencies()
    go_register_toolchains(version = "1.24.0")
    gazelle_dependencies(go_sdk = "go_sdk")
