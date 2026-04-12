load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "http",
    srcs = [
        "HealthCheckRequestHandler.java",
        "HealthCheckServer.java",
        "HttpRequestHelper.java",
        "ReadinessCheckRequestHandler.java",
        "ReadinessChecker.java",
    ],
    visibility = ["//:__subpackages__"],
    deps = [
        "//src/main/java/com/xgen/mongot/server/grpc",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_apache_httpcomponents_httpclient",
        "@maven//:org_apache_httpcomponents_httpcore",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
