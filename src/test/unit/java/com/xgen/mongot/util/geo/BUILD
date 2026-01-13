load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "geo",
    srcs = [
        "GeoJsonParserTest.java",
        "GeometryFlattenerTest.java",
    ],
    data = [
        "//src/test/unit/resources/util/geo:srcs",
    ],
    tags = ["unit"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/mongot/util/geo",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/util/geo",
        "@maven//:com_google_guava_guava",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
