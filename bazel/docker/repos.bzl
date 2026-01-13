load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def docker_repos():
    http_archive(
        name = "rules_oci",
        sha256 = "5994ec0e8df92c319ef5da5e1f9b514628ceb8fc5824b4234f2fe635abb8cc2e",
        strip_prefix = "rules_oci-2.2.6",
        url = "https://github.com/bazel-contrib/rules_oci/releases/download/v2.2.6/rules_oci-v2.2.6.tar.gz",
    )

    git_repository(
        name = "distroless",
        commit = "033387ac8853e6cc1cd47df6c346bc53cbc490d8",
        remote = "https://github.com/GoogleCloudPlatform/distroless.git",
        shallow_since = "1551312943 -0600",
    )
