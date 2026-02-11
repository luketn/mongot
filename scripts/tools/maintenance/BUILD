py_library(
    name = "maintenance",
    srcs = glob(["*.py"]),
    srcs_version = "PY3",
    visibility = ["//visibility:public"],
)

py_binary(
    name = "create_force_merge_deletes_request",
    srcs = [
        "create_force_merge_deletes_request.py",
        "create_request_util.py",
    ],
    main = "create_force_merge_deletes_request.py",
    python_version = "PY3",
)

py_binary(
    name = "create_clear_index_request",
    srcs = [
        "create_clear_index_request.py",
        "create_request_util.py",
    ],
    main = "create_clear_index_request.py",
    python_version = "PY3",
)
