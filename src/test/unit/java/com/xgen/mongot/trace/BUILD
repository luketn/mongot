load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "trace",
    srcs = [
        "ExporterTest.java",
        "TracingTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/trace",
        "//src/main/java/com/xgen/testing",
        "@maven//:ch_qos_logback_logback_classic",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:io_opentelemetry_opentelemetry_api",
        "@maven//:io_opentelemetry_opentelemetry_context",
        "@maven//:junit_junit",
    ],
)
