load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "steadystate",
    srcs = [
        "SteadyStateIndexManagerTest.java",
        "SteadyStateManagerTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/replication/mongodb/common",
        "//src/main/java/com/xgen/mongot/replication/mongodb/steadystate",
        "//src/main/java/com/xgen/mongot/replication/mongodb/steadystate/changestream",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing/mongot/index",
        "//src/main/java/com/xgen/testing/mongot/index/version",
        "//src/main/java/com/xgen/testing/mongot/mock/index",
        "//src/main/java/com/xgen/testing/mongot/mock/replication/mongodb/common",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
