load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "shapes",
    srcs = ["GeoShapeTest.java"],
    data = ["//src/test/unit/resources/index/query/shapes:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query/shapes",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/query/shapes",
        "//src/main/java/com/xgen/testing/mongot/util/geo",
        "@maven//:junit_junit",
    ],
)
