load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def python_repos():
    http_archive(
        name = "rules_python",
        sha256 = "9f9f3b300a9264e4c77999312ce663be5dee9a56e361a1f6fe7ec60e1beef9a3",
        strip_prefix = "rules_python-1.4.1",
        url = "https://github.com/bazel-contrib/rules_python/releases/download/1.4.1/rules_python-1.4.1.tar.gz",
    )
