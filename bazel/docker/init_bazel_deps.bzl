load("@rules_oci//oci:repositories.bzl", "oci_register_toolchains")

def docker_init_bazel_deps():
    oci_register_toolchains(name = "oci")
