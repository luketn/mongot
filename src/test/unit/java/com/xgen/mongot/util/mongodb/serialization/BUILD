load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "serialization",
    srcs = ["MongoDbCollectionInfoTest.java"],
    data = ["//src/test/unit/resources/util/mongodb/serialization:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/mongodb/serialization",
        "//src/main/java/com/xgen/testing",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
