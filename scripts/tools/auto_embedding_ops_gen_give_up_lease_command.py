#!/usr/bin/env python3
"""
Generate the admin command JSON for the auto-embedding ops give-up lease (rebalance) tool.
Uses UTC for expiresAt to avoid timezone mismatches.

Example output (included in runbook):
{
  "additionalParams": {
    "autoEmbedding": {
      "opsGiveUpLease": {
        "expiresAt": "2026-03-10T12:00:00Z",
        "instance": "atlas-ujtvbt-shard-00-02.ytzdfr.mongodb-dev.net",
        "leaseNames": [
          "69a911de47bae66db9a469f2-7d069980f686789aa013f615f318a6e3-1",
          "69a911cf47bae66db9a46832-30609c9d471c4d0ec6114b7b32749eca-1"
        ]
      }
    }
  }
}
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate ops give-up lease admin command JSON (UTC expiresAt)."
    )
    parser.add_argument(
        "instance",
        help="Agent hostname of the mongot that should give up the leases (e.g. atlas-ujtvbt-shard-00-02.ytzdfr.mongodb-dev.net)",
    )
    parser.add_argument(
        "lease_names",
        nargs="+",
        help="Lease names (MV collection names) to give up",
    )
    parser.add_argument(
        "--expires-in-minutes",
        type=int,
        default=60,
        metavar="N",
        help="Expiration time from now in minutes (default: 60). expiresAt is set in UTC.",
    )
    args = parser.parse_args()

    now_utc = datetime.now(timezone.utc)
    expires_at_utc = now_utc + timedelta(minutes=args.expires_in_minutes)
    expires_at_str = expires_at_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    payload = {
        "additionalParams": {
            "autoEmbedding": {
                "opsGiveUpLease": {
                    "expiresAt": expires_at_str,
                    "instance": args.instance,
                    "leaseNames": args.lease_names,
                }
            }
        }
    }

    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
