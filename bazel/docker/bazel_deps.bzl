load("@rules_oci//oci:dependencies.bzl", "rules_oci_dependencies")

def docker_bazel_deps():
    rules_oci_dependencies()
