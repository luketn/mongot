load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
load("@rules_buf//buf:repositories.bzl", "rules_buf_dependencies", "rules_buf_toolchains")
load("@rules_buf//gazelle/buf:repositories.bzl", "gazelle_buf_dependencies")
load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies")

def proto_bazel_deps():
    protobuf_deps()

    rules_proto_dependencies()

    rules_buf_dependencies()
    rules_buf_toolchains(version = "v1.31.0")
    gazelle_buf_dependencies()
