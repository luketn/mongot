load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

RULES_JVM_EXTERNAL_TAG = "6.7"
RULES_JVM_EXTERNAL_SHA = "a1e351607f04fed296ba33c4977d3fe2a615ed50df7896676b67aac993c53c18"

def java_repos():
    # Note: rules_shell is loaded in bazel/shell/repos.bzl (required for rules_jvm 6.7+)

    # An explicit dependency on rules_license is necessary for rules_jvm 6.3+
    http_archive(
        name = "rules_license",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/rules_license/releases/download/1.0.0/rules_license-1.0.0.tar.gz",
            "https://github.com/bazelbuild/rules_license/releases/download/1.0.0/rules_license-1.0.0.tar.gz",
        ],
        sha256 = "26d4021f6898e23b82ef953078389dd49ac2b5618ac564ade4ef87cced147b38",
    )

    # Pin rules_java to 8.5.1 for Bazel 8.0.0 compatibility
    http_archive(
        name = "rules_java",
        urls = [
            "https://github.com/bazelbuild/rules_java/releases/download/8.5.1/rules_java-8.5.1.tar.gz",
        ],
        sha256 = "1389206b2208c5f33a05dd96e51715b0855c480c082b7bb4889a8e07fcff536c",
    )

    http_archive(
        name = "apple_rules_lint",
        strip_prefix = "apple_rules_lint-0.4.0",
        sha256 = "483ea03d73d5fb33275d029da8d36811243fc32dfa4dc73a43acbb6f4b1af621",
        url = "https://github.com/apple/apple_rules_lint/releases/download/0.4.0/apple_rules_lint-0.4.0.tar.gz",
    )

    # 0.27.0 is the final version that doesn't require migrating to bzlmod
    http_archive(
        name = "contrib_rules_jvm",
        sha256 = "e6cd8f54b7491fb3caea1e78c2c740b88c73c7a43150ec8a826ae347cc332fc7",
        strip_prefix = "rules_jvm-0.27.0",
        url = "https://github.com/bazel-contrib/rules_jvm/releases/download/v0.27.0/rules_jvm-v0.27.0.tar.gz",
    )

    http_archive(
        name = "rules_jvm_external",
        strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
        sha256 = RULES_JVM_EXTERNAL_SHA,
        url = "https://github.com/bazelbuild/rules_jvm_external/releases/download/%s/rules_jvm_external-%s.tar.gz" % (RULES_JVM_EXTERNAL_TAG, RULES_JVM_EXTERNAL_TAG),
    )

    git_repository(
        name = "io_grpc_grpc_java",
        remote = "https://github.com/grpc/grpc-java.git",
        # v1.70.0,
        commit = "22a42c88d80fb96326b8c507591e4017c5e15ed3",
        shallow_since = "1737476933 +0000",
    )
