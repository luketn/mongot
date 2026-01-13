load("@rules_java//java:rules_java_deps.bzl", "rules_java_dependencies")

def java_early_init():
    """Initialize rules_java dependencies early.

    This must be called immediately after loading repos, before other bazel_deps.
    Required for rules_java 8.5.1+ with Bazel 8.0.0 to set up compatibility_proxy.
    """
    rules_java_dependencies()
