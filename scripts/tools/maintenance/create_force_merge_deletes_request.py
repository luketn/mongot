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
import sys
from pathlib import Path

# Add the script's directory to the path for imports
sys.path.insert(0, str(Path(__file__).parent))

import create_request_util


def usage(script_name):
    return f"""Usage: {script_name} --generation-id <id> --index-def <path> [options]

Required:
  --generation-id <id>        Index generation ID
  --index-def <path>          Path to JSON file with full index definition
  --requested-at <string>     Timestamp
  --requested-by <string>     Who created the request
  --reason <string>           Reason for the request

Optional:
  --percentage <0.0-100.0>    Force merge deletes percentage. Segments with
                               deletes above this threshold will be force merged.
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

    create_request_util.add_metadata_arguments(parser)
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

    output_file = create_request_util.set_output_file(args.output,
                                                      args.generation_id)

    create_request_util.add_metadata_to_request(request_json, args)
    create_request_util.write_request_file(request_json, output_file)


if __name__ == "__main__":
    main()
