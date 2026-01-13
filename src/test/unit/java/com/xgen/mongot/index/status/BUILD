load("//bazel/java:test.bzl", "mongot_java_unit_test_suite")

# gazelle:ignore

mongot_java_unit_test_suite(
    name = "lib",
    resources = True,
    deps = [
        "org.mongodb:bson",
        "//src/main/java/com/xgen/mongot/index/status",
        "//src/main/java/com/xgen/testing",
    ],
)
