load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "http",
    srcs = ["HealthCheckServerTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/server/grpc",
        "//src/main/java/com/xgen/mongot/server/http",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
    ],
)
