#!/usr/bin/env python3
#
# Script to create a clear index request JSON file. The generated JSON file should be provided to
# cloud operations engineers (COE) for submission to the search node.
#
# Usage:
#   create_clear_index_request.py --generation-id <id> [options]
#

import argparse
import sys
from pathlib import Path

# Add the script's directory to the path for imports
sys.path.insert(0, str(Path(__file__).parent))

import create_request_util


def usage(script_name):
    return f"""Usage: {script_name} --generation-id <id> [options]

Required:
  --generation-id <id>        Index generation ID
  --requested-at <string>     Timestamp
  --requested-by <string>     Who created the request
  --reason <string>           Reason for the request

Optional:
  --output <path>             Output file path (default:
                               ./request_<generation-id>_<timestamp>.json)

The generated file should be provided to cloud operations engineers for
submission to the search node."""


def main():
    parser = argparse.ArgumentParser(
        description="Create a clear index request JSON file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=usage(sys.argv[0]))

    parser.add_argument(
        "--generation-id",
        required=True,
        help="Index generation ID")

    create_request_util.add_metadata_arguments(parser)
    args = parser.parse_args()

    # Build request JSON
    request_json = {
        "tool": "CLEAR_INDEX",
        "generationId": args.generation_id,
    }

    create_request_util.add_metadata_to_request(request_json, args)

    output_file = create_request_util.set_output_file(args.output,
                                                      args.generation_id)

    create_request_util.write_request_file(request_json, output_file)


if __name__ == "__main__":
    main()
