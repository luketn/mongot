VersionInfo = provider(fields = ["type"])

def _version_impl(ctx):
    return VersionInfo(type = ctx.build_setting_value)

version = rule(
    implementation = _version_impl,
    build_setting = config.string(flag = True),
)
