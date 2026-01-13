#!/usr/bin/env python3
#
# Script to create a force merge deletes request JSON file. The generated JSON file should be
# provided to cloud operations engineers (COE) for submission to the search node.
#
# Usage:
#   create_force_merge_deletes_request.py --generation-id <id> --index-def <path> [options]
#

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path


def usage(script_name):
    return f"""Usage: {script_name} --generation-id <id> --index-def <path> [options]

Required:
  --generation-id <id>        Index generation ID
  --index-def <path>          Path to JSON file with full index definition

Optional:
  --percentage <0.0-100.0>    Force merge deletes percentage. Segments with
                               deletes above this threshold will be force merged.
  --requested-at <string>     Timestamp
  --requested-by <string>     Who created the request
  --reason <string>           Reason for the request
  --output <path>             Output file path (default:
                               ./request_<generation-id>_<timestamp>.json)

Note: The index definition must be obtained from the internal MongoDB cluster.
See README.md for instructions on how to query and extract the index definition.

The generated file should be provided to cloud operations engineers for
submission to the search node."""


def validate_percentage(percentage_str):
    """Validate that percentage is a number between 0.0 and 100.0."""
    try:
        percentage = float(percentage_str)
    except ValueError:
        raise ValueError("Percentage must be a number between 0.0 and 100.0")
    if not (0.0 <= percentage <= 100.0):
        raise ValueError("Percentage must be between 0.0 and 100.0")
    return percentage


def validate_index_definition(index_def):
    """
    Validate that index definition has exactly one of searchIndexDefinition or
    vectorIndexDefinition.
    """
    has_vector = "vectorIndexDefinition" in index_def
    has_search = "searchIndexDefinition" in index_def

    if (has_vector and has_search) or (not has_vector and not has_search):
        print(
            "Error: Index definition must have exactly one of"
            " 'searchIndexDefinition' (search) or"
            " 'vectorIndexDefinition' (vector)",
            file=sys.stderr)
        print("Expected format from aggregation pipeline:", file=sys.stderr)
        print('  - Search: { "searchIndexDefinition": { ... } }', file=sys.stderr)
        print('  - Vector: { "vectorIndexDefinition": { ... } }', file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Create a force merge deletes request JSON file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=usage(sys.argv[0]))

    parser.add_argument(
        "--generation-id",
        required=True,
        help="Index generation ID")
    parser.add_argument(
        "--index-def",
        required=True,
        type=Path,
        help="Path to JSON file with full index definition")
    parser.add_argument(
        "--percentage",
        type=str,
        help="Force merge deletes percentage (0.0-100.0)")
    parser.add_argument(
        "--requested-at",
        type=str,
        help="Timestamp")
    parser.add_argument(
        "--requested-by",
        type=str,
        help="Who created the request")
    parser.add_argument(
        "--reason",
        type=str,
        help="Reason for the request")
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Output file path (default: "
            "./request_<generation-id>_<timestamp>.json)"))

    args = parser.parse_args()

    # Validate percentage if provided
    percentage_value = None
    if args.percentage:
        try:
            percentage_value = validate_percentage(args.percentage)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(
            "Note: --percentage not provided, the default value of the tool will be used",
            file=sys.stderr)

    # Set default output file if not specified
    if args.output:
        output_file = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # When running via bazel run, use the repository root if available,
        # otherwise use current working directory
        repo_root = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
        if repo_root:
            output_file = Path(repo_root) / (
                f"request_{args.generation_id}_{timestamp}.json")
        else:
            output_file = Path(f"request_{args.generation_id}_{timestamp}.json")
        print(f"Note: Using default output file: {output_file}", file=sys.stderr)

    # Validate and load index definition
    index_def_path = args.index_def
    if not index_def_path.exists():
        print(
            f"Error: Index definition file not found: {index_def_path}",
            file=sys.stderr)
        sys.exit(1)

    try:
        with open(index_def_path) as f:
            index_def_content = json.load(f)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON: {index_def_path}", file=sys.stderr)
        sys.exit(1)
    except OSError as e:
        print(f"Error: Failed to read index definition file: {e}", file=sys.stderr)
        sys.exit(1)

    # Validate the index definition format
    validate_index_definition(index_def_content)

    # Build request JSON
    request_json = {
        "tool": "FORCE_MERGE_DELETES",
        "generationId": args.generation_id,
    }

    # Add the index definition (either searchIndexDefinition or
    # vectorIndexDefinition)
    if "searchIndexDefinition" in index_def_content:
        request_json["searchIndexDefinition"] = (
            index_def_content["searchIndexDefinition"])
    else:
        request_json["vectorIndexDefinition"] = (
            index_def_content["vectorIndexDefinition"])

    # Add optional percentage field if provided
    if percentage_value is not None:
        request_json["forceMergeDeletesPctAllowed"] = percentage_value

    # Add metadata if any field is provided
    if args.requested_at or args.requested_by or args.reason:
        metadata = {}
        if args.requested_at:
            metadata["requestedAt"] = args.requested_at
        if args.requested_by:
            metadata["requestedBy"] = args.requested_by
        if args.reason:
            metadata["reason"] = args.reason
        request_json["metadata"] = metadata
    else:
        print(
            "Note: No metadata fields provided, request will not include metadata",
            file=sys.stderr)

    # Write file
    try:
        # Ensure parent directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(request_json, f, indent=2)
            f.write("\n")
    except OSError as e:
        print(f"Error: Failed to write output file: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Request created: {output_file}")
    print("")
    print("Next steps:")
    print("  1. Review the generated JSON file")
    print("  2. Provide this file to cloud operations engineers")
    print("  3. COE will place it in: <mongot-data-path>/maintenance/pending/")


if __name__ == "__main__":
    main()
