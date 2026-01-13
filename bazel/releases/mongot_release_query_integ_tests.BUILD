load("@rules_pkg//:pkg.bzl", "pkg_tar")
load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_files", "strip_prefix")

pkg_files(
    name = "files",
    srcs = glob(["*.json"]),
    attributes = pkg_attributes(mode = "0755"),
    strip_prefix = strip_prefix.from_pkg(),
    visibility = ["//visibility:public"],
)

pkg_tar(
    name = "tarball",
    srcs = [":files"],
    extension = "tgz",
    strip_prefix = ".",
    visibility = ["//visibility:public"],
)
