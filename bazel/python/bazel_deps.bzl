load("@rules_python//python:repositories.bzl", "py_repositories", "python_register_toolchains")

def python_bazel_deps():
    py_repositories()
    python_register_toolchains(
        name = "python_3_11",
        python_version = "3.11",
    )
