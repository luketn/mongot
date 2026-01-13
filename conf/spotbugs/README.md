# SpotBugs Static Analysis in Mongot

SpotBugs is a static analysis tool that detects potential bugs in Java code. This directory contains the configuration for SpotBugs integration in the mongot project.

## Overview

SpotBugs analyzes Java bytecode to find potential bugs, performance issues, and code quality problems. The mongot project uses SpotBugs 4.9.8, which supports Java 21 (class file major version 65).

## Running SpotBugs

SpotBugs runs automatically as part of the lint tests:

```bash
# Run all lint tests (including SpotBugs and Checkstyle)
make lint

# Run SpotBugs on a specific target
bazel test --build_tests_only --test_tag_filters=spotbugs //path/to/target:all

# Run SpotBugs on all Java code
bazel test --build_tests_only --test_tag_filters=spotbugs //src/...
```

## Configuration

### BUILD File

The `BUILD` file defines:
- `spotbugs_binary`: The SpotBugs CLI tool with Maven dependencies
- `spotbugs_config`: Configuration for SpotBugs analysis
  - `effort`: Analysis effort level (default, min, or max)
  - `exclude_filter`: XML file with exclusion rules
  - `fail_on_warning`: Whether to fail the build on warnings (set to `true`)

### Exclusion Rules

The `spotbugs-exclude.xml` file contains patterns for suppressing specific warnings. Add exclusions here when:
- A warning is a false positive
- The code pattern is intentional and safe
- The warning applies to generated or third-party code

Example exclusion:
```xml
<Match>
    <Bug pattern="PATTERN_NAME"/>
    <Class name="~.*Test"/>
</Match>
```

## Common Bug Patterns

SpotBugs detects many types of issues, including:

- **Dm_DEFAULT_ENCODING**: Reliance on default character encoding
  - Fix: Specify charset explicitly (e.g., `StandardCharsets.UTF_8`)
- **EI_EXPOSE_REP**: Exposing internal representation
  - Fix: Return defensive copies of mutable objects
- **NP_NULL_ON_SOME_PATH**: Possible null pointer dereference
  - Fix: Add null checks or use `@Nullable` annotations
- **RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE**: Redundant null check
  - Fix: Remove unnecessary null checks

## Suppressing Warnings

### In Code

Use SpotBugs annotations to suppress warnings:

```java
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATTERN_NAME", justification = "Explanation here")
public void myMethod() {
    // code
}
```

### In Configuration

Add patterns to `spotbugs-exclude.xml` for broader suppressions.

## Integration Details

SpotBugs is integrated via:
1. Maven dependency in `bazel/java/deps.bzl` (version 4.9.8)
2. Bazel rules in `conf/spotbugs/BUILD`
3. Lint setup in `bazel/java/deps.bzl` (`lint_setup`)

The integration uses `@contrib_rules_jvm` for Bazel support, similar to Checkstyle.

## Java 21 Support

SpotBugs 4.8.0+ is required for Java 21 support. Earlier versions fail with:
```
java.lang.IllegalArgumentException: Unsupported class file major version 65
```

The mongot project uses SpotBugs 4.9.8, which fully supports Java 21.

## Resources

- [SpotBugs Documentation](https://spotbugs.readthedocs.io/)
- [Bug Patterns](https://spotbugs.readthedocs.io/en/stable/bugDescriptions.html)
- [SpotBugs Annotations](https://javadoc.io/doc/com.github.spotbugs/spotbugs-annotations/latest/index.html)

