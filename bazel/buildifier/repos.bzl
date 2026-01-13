load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def buildifier_repos():
    http_archive(
        name = "com_github_bazelbuild_buildtools",
        strip_prefix = "buildtools-7.3.1",
        url = "https://github.com/bazelbuild/buildtools/archive/refs/tags/v7.3.1.zip",
        sha256 = "118602587d5804c720c1617db30f56c93ec7a2bdda5e915125fccf7421e78412",
    )
