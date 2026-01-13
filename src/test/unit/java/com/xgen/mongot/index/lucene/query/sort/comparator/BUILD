load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "comparator",
    srcs = [
        "DoubleFieldConverterTest.java",
        "MqlLongComparatorTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/query/sort/comparator",
        "//src/main/java/com/xgen/mongot/index/lucene/util",
        "//src/main/java/com/xgen/mongot/index/query/sort",
        "//src/main/java/com/xgen/testing",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
