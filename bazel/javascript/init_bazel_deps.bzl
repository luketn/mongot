load("@aspect_rules_js//js:toolchains.bzl", "rules_js_register_toolchains")
load("@rules_nodejs//nodejs:repositories.bzl", "nodejs_register_toolchains")

def javascript_init_bazel_deps():
    # Register Node.js toolchains
    nodejs_register_toolchains(
        name = "nodejs",
        node_version = "20.11.0",  # LTS version
    )

    # Register rules_js toolchains
    rules_js_register_toolchains()
