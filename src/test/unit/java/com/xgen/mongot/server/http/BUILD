load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "http",
    srcs = [
        "HealthCheckRequestHandlerTest.java",
        "HealthCheckServerTest.java",
        "HttpRequestHelperTest.java",
        "ReadinessCheckRequestHandlerTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/server/grpc",
        "//src/main/java/com/xgen/mongot/server/http",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
    ],
)
