load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "pushdown",
    srcs = [
        "ArrayComparatorTest.java",
        "MqlComparatorTest.java",
        "MqlDoubleTest.java",
        "MqlNumberUtilsTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/query/pushdown",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
