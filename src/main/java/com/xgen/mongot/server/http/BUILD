load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "http",
    srcs = ["HealthCheckServer.java"],
    visibility = ["//:__subpackages__"],
    deps = [
        "//src/main/java/com/xgen/mongot/server/grpc",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_google_guava_guava",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
