load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "system",
    srcs = ["SystemMetricsInstrumentationTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics/system",
        "@maven//:com_google_flogger_flogger_system_backend",  # keep
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
    ],
)
