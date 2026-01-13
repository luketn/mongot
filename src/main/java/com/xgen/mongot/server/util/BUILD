load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "util",
    srcs = ["NettyUtil.java"],
    visibility = ["//:__subpackages__"],
    exports = ["@maven//:io_netty_netty_transport"],
    deps = [
        "//bazel/java:netty-transport-native-epoll",  # keep: netty native transport
        "//bazel/java:netty-transport-native-kqueue",  # keep: netty native transport
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:io_netty_netty_transport",
        "@maven//:io_netty_netty_transport_classes_epoll",
        "@maven//:io_netty_netty_transport_classes_kqueue",
        "@maven//:io_netty_netty_transport_native_unix_common",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
