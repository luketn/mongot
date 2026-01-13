load("@rules_shell//shell:repositories.bzl", "rules_shell_dependencies", "rules_shell_toolchains")

def shell_bazel_deps():
    """Initialize rules_shell dependencies and toolchains."""
    rules_shell_dependencies()
    rules_shell_toolchains()
