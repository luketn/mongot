#!/usr/bin/env python3
#
# Utility functions for creating maintenance request JSON files.
#

import json
import os
import sys
from datetime import datetime
from pathlib import Path


def add_metadata_arguments(parser):
    parser.add_argument(
        "--requested-at",
        type=str,
        required=True,
        help="Timestamp")
    parser.add_argument(
        "--requested-by",
        type=str,
        required=True,
        help="Who created the request")
    parser.add_argument(
        "--reason",
        type=str,
        required=True,
        help="Reason for the request")
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Output file path (default: "
            "./request_<generation-id>_<timestamp>.json)"))


def add_metadata_to_request(request_json, args):
    # Add metadata if any field is provided
    metadata = {
        "requestedAt": args.requested_at,
        "requestedBy": args.requested_by,
        "reason": args.reason}
    request_json["metadata"] = metadata


def set_output_file(output, generation_id):
    # When running via bazel run, use the repository root as the base directory
    repo_root = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    base_dir = Path(repo_root) if repo_root else Path.cwd()

    if output:
        # If output is a relative path, resolve it relative to the base directory
        if not output.is_absolute():
            output_file = base_dir / output
        else:
            output_file = output
        return output_file

    # Set default output file if not specified
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = base_dir / f"request_{generation_id}_{timestamp}.json"
    print(f"Note: Using default output file: {output_file}", file=sys.stderr)

    return output_file


def write_request_file(request_json, output_file):
    # Write file
    try:
        # Ensure parent directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w+") as f:
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
