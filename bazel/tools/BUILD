alias(
    name = "promtool",
    actual = select({
        "//bazel/platforms:is_linux_aarch64": "@prometheus_linux_aarch64//:bin",
        "//bazel/platforms:is_linux_x86_64": "@prometheus_linux_x86_64//:bin",
        "//bazel/platforms:is_macos_aarch64": "@prometheus_macos_aarch64//:bin",
        "//bazel/platforms:is_macos_x86_64": "@prometheus_macos_x86_64//:bin",
    }),
    visibility = ["//visibility:public"],
)
