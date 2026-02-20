load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def javascript_repos():
    http_archive(
        name = "aspect_rules_js",
        sha256 = "1774702556e1d0b83b7f5eb58ec95676afe6481c62596b53f5b96575bacccf73",
        strip_prefix = "rules_js-2.9.2",
        url = "https://github.com/aspect-build/rules_js/releases/download/v2.9.2/rules_js-v2.9.2.tar.gz",
    )

    http_archive(
        name = "rules_nodejs",
        sha256 = "32894b914e4aed4245afdcece6fad94413f7f86eaefb863ff71e8ba6e5992b6b",
        strip_prefix = "rules_nodejs-6.7.3",
        url = "https://github.com/bazel-contrib/rules_nodejs/releases/download/v6.7.3/rules_nodejs-v6.7.3.tar.gz",
    )
