# promtool/defs.bzl

def _promtool_test_impl(ctx):
    promtool = ctx.executable._promtool
    rules = ctx.files.rules
    tests = ctx.files.tests

    output = ctx.outputs.executable

    command = """
        set -e
        {promtool} test rules {tests}
        touch {output}
    """.format(
        promtool = promtool.path,
        tests = " ".join([t.path for t in tests]),
        output = output.path,
    )

    # Combine all file lists and create a single depset for the inputs.
    all_inputs = rules + tests + [promtool]

    ctx.actions.run_shell(
        outputs = [output],
        inputs = depset(all_inputs),
        command = command,
        mnemonic = "PromtoolTest",
        progress_message = "Running promtool tests on %{label}",
    )

    return [
        DefaultInfo(
            executable = output,
        ),
    ]

promtool_test = rule(
    implementation = _promtool_test_impl,
    test = True,
    attrs = {
        "rules": attr.label_list(
            doc = "List of Prometheus rule files.",
            allow_files = True,
            mandatory = True,
        ),
        "tests": attr.label_list(
            doc = "List of test files for the Prometheus rules.",
            allow_files = True,
            mandatory = True,
        ),
        "_promtool": attr.label(
            doc = "The promtool executable.",
            cfg = "exec",
            executable = True,
            allow_files = True,
            default = Label("//bazel/tools:promtool"),
        ),
    },
)
