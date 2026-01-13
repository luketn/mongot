load("//bazel/java:test.bzl", "mongot_java_unit_test_suite")

# Other tests depend directly on TestServerStatusDataExtractor as a library.
# gazelle:ignore

mongot_java_unit_test_suite(
    name = "lib",
    deps = [
        "org.apache.commons:commons-lang3",
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/functionalinterfaces",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/metrics/micrometer",
        "//src/main/java/com/xgen/testing/mongot/mock/index",
        "@maven//:com_google_guava_guava_testlib",
        "@maven//:io_micrometer_micrometer_registry_prometheus",
    ],
)
