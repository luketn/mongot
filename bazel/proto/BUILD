load("@bazel_gazelle//:def.bzl", "gazelle", "gazelle_binary")

# TODO: this is included to ensure it will work, but should not be used
# until proto files are refactored to roughly match what buf expects,
# i.e., under a single `proto` directory structure
gazelle(
    name = "gazelle",
    gazelle = ":gazelle-buf",
)

gazelle_binary(
    name = "gazelle-buf",
    languages = [
        "@bazel_gazelle//language/proto:go_default_library",
        "@rules_buf//gazelle/buf:buf",
    ],
)
