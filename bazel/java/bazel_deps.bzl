load("@apple_rules_lint//lint:repositories.bzl", "lint_deps")
load("@contrib_rules_jvm//:repositories.bzl", "contrib_rules_jvm_deps", "contrib_rules_jvm_gazelle_deps")
load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")

def java_bazel_deps():
    lint_deps()
    contrib_rules_jvm_deps()
    contrib_rules_jvm_gazelle_deps()
    grpc_java_repositories()
