#!/usr/bin/env python3.10

"""Verify rank selections from daos-pool-balancer output.

This tool validates that each selected fault domain has the expected number
of selected ranks (default: 2), and prints selected rank counts per Aurora
128-node group.
"""

import argparse
import collections
import json
import re
import subprocess
import sys

GROUP_SIZE = 128
NODE_PATTERN = re.compile(r"aurora-daos-(\d+)$")


def parse_ranks(ranks_arg):
    """Parse a comma-separated rank list into sorted unique integers."""
    ranks = set()
    for token in ranks_arg.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            ranks.add(int(token))
        except ValueError as exc:
            raise ValueError(f"Invalid rank '{token}' in --ranks") from exc

    if not ranks:
        raise ValueError("--ranks did not contain any valid rank values")

    return sorted(ranks)


def load_system_query(path):
    """Load system query JSON either from file or dmg."""
    if path:
        with open(path, "rb") as handle:
            return json.loads(handle.read())

    result = subprocess.run(
        ["sudo", "dmg", "system", "query", "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def fault_domain_to_host(fault_domain):
    """Normalize DAOS fault domain string to host-like string."""
    return fault_domain.lstrip("/")


def group_label_for_host(host):
    """Return Aurora group label for a host name."""
    match = NODE_PATTERN.search(host)
    if not match:
        return "unmapped"

    node_num = int(match.group(1))
    group_idx = (node_num - 1) // GROUP_SIZE
    start = group_idx * GROUP_SIZE + 1
    end = start + GROUP_SIZE - 1
    return f"aurora-daos-[{start:04d}-{end:04d}]"


def build_rank_maps(system_query):
    """Build lookup tables from system query JSON."""
    members = system_query["response"]["members"]

    rank_to_fault_domain = {}
    rank_to_group = {}
    for item in members:
        rank = int(item["rank"])
        fault_domain = item["fault_domain"]
        host = fault_domain_to_host(fault_domain)

        rank_to_fault_domain[rank] = fault_domain
        rank_to_group[rank] = group_label_for_host(host)

    return rank_to_fault_domain, rank_to_group


def verify_selection(selected_ranks, rank_to_fault_domain, rank_to_group, expected_per_node):
    """Verify per-node selection count and compute group totals."""
    missing = [rank for rank in selected_ranks if rank not in rank_to_fault_domain]
    if missing:
        print("error: selected ranks not found in system query: {}".format(
            ",".join(map(str, sorted(missing)))
        ))
        return 2

    per_domain = collections.Counter()
    per_group = collections.Counter()

    for rank in selected_ranks:
        fault_domain = rank_to_fault_domain[rank]
        per_domain[fault_domain] += 1
        per_group[rank_to_group[rank]] += 1

    print("selected_ranks_total: {}".format(len(selected_ranks)))

    bad_domains = []
    for fault_domain in sorted(per_domain.keys()):
        count = per_domain[fault_domain]
        if count != expected_per_node:
            bad_domains.append((fault_domain, count))

    if bad_domains:
        print("node_pairing_check: FAIL")
        print("domains_with_unexpected_rank_count:")
        for fault_domain, count in bad_domains:
            print("  {} -> {}".format(fault_domain, count))
        status = 1
    else:
        print("node_pairing_check: PASS")
        status = 0

    print("ranks_per_group:")
    ordered_labels = [
        "aurora-daos-[0001-0128]",
        "aurora-daos-[0129-0256]",
        "aurora-daos-[0257-0384]",
        "aurora-daos-[0385-0512]",
        "aurora-daos-[0513-0640]",
        "aurora-daos-[0641-0768]",
        "aurora-daos-[0769-0896]",
        "aurora-daos-[0897-1024]",
    ]

    for label in ordered_labels:
        print("  {}: {}".format(label, per_group.get(label, 0)))

    unmapped = per_group.get("unmapped", 0)
    if unmapped:
        print("  unmapped: {}".format(unmapped))

    return status


def main():
    parser = argparse.ArgumentParser(description="Verify DAOS pool balancer rank selection")
    parser.add_argument(
        "--ranks",
        required=True,
        help="Comma-separated selected rank list",
    )
    parser.add_argument(
        "--expected-ranks-per-node",
        type=int,
        default=2,
        help="Expected number of selected ranks per selected fault domain (default: 2)",
    )
    parser.add_argument(
        "--system-query-json",
        default="",
        help="Optional path to a saved dmg system query --json output",
    )

    args = parser.parse_args()

    try:
        selected_ranks = parse_ranks(args.ranks)
        system_query = load_system_query(args.system_query_json or None)
        rank_to_fault_domain, rank_to_group = build_rank_maps(system_query)
    except (ValueError, KeyError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        print(f"error: {error}")
        return 2

    return verify_selection(
        selected_ranks,
        rank_to_fault_domain,
        rank_to_group,
        args.expected_ranks_per_node,
    )


if __name__ == "__main__":
    sys.exit(main())
