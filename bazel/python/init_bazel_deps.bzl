load("@rules_python//python:pip.bzl", "pip_parse")

def python_init_bazel_deps():
    pip_parse(
        name = "pip_deps",
        requirements_lock = "//bazel/python:requirements_lock.txt",
    )
