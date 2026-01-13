load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def go_repos():
    http_archive(
        name = "io_bazel_rules_go",
        sha256 = "b78f77458e77162f45b4564d6b20b6f92f56431ed59eaaab09e7819d1d850313",
        urls = [
            "https://mirror.bazel.build/github.com/bazel-contrib/rules_go/releases/download/v0.53.0/rules_go-v0.53.0.zip",
            "https://github.com/bazel-contrib/rules_go/releases/download/v0.53.0/rules_go-v0.53.0.zip",
        ],
    )

    http_archive(
        name = "bazel_gazelle",
        integrity = "sha256-XYDmKnAxTznMdkwcPqqADFk2yfHqkWJQBiJ85NIM0IY=",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.42.0/bazel-gazelle-v0.42.0.tar.gz",
            "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.42.0/bazel-gazelle-v0.42.0.tar.gz",
        ],
    )
