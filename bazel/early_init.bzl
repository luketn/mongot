load("//bazel/java:early_init.bzl", "java_early_init")

def mongot_early_init():
    """Early initialization that must happen right after repos are loaded.

    This is required for Bazel 8.0.0 compatibility with WORKSPACE mode.
    """
    java_early_init()
