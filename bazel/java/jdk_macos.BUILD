load("@rules_pkg//:pkg.bzl", "pkg_tar")
load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_files", "strip_prefix")

pkg_files(
    name = "jdk",
    srcs = glob(
        ["**"],
        exclude = [
            "BUILD.bazel",
            "WORKSPACE",
            "MODULE.bazel",
            "MODULE.bazel.lock",
        ],
    ),
    attributes = pkg_attributes(mode = "0755"),
    prefix = "jdk",
    strip_prefix = strip_prefix.from_pkg(),
    visibility = ["//visibility:public"],
)

pkg_tar(
    name = "tar",
    srcs = [":jdk"],
    strip_prefix = ".",
    visibility = ["//visibility:public"],
)

filegroup(
    name = "srcs",
    srcs = glob(
        ["Contents/**"],
        exclude = [
            "Contents/Home/demo/**",
            "Contents/Home/man/**",
            "Contents/Home/legal/**",
        ],
    ),
    visibility = ["//visibility:public"],
)

filegroup(
    name = "java",
    srcs = ["Contents/Home/bin/java"],
    visibility = ["//visibility:public"],
)
