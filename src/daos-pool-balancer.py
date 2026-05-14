#!/usr/bin/env python3.10

#
# This script outputs a pool creation command which creates a pool on a specific set of ranks rather than all.
# 
# The ranks are selected according to the following:
#    - determines the number of total ranks in the pool by 
#      - selecting the minimum of either:
#        - each rank has at least <min_bytes_per_rank> 
#        - the maximum number of ranks based on <max_ratio> * the total ranks
#    - selects a random starting dragonfly group
#    - until all ranks selected
#      - select the rank with the maximum free bytes off NVM
#      - moves to the next dragonfly group in sequence
#    
# Currently, SCM is not considered in the selection.
#
# Example:
#   daos-pool-balancer.py --pool=POOL --user=USER --group=GROUP --size=10.0T
#   dmg pool create --properties=rd_fac:3,space_rb:8 --user=USER --group=GROUP --size=10.0T --ranks="17,16,39,38,37,36,35,34,33,32,31,30,29,28,27,26,25,24,23,22,21,20,18,15" POOL
#


import argparse
import collections
import json
import subprocess
import random

test = False

# determined by fault domain
aurora_server_to_group = 64

# set by querying system
max_ranks = 0

# need to decide how to compute
min_bytes_per_rank = 1024*1024*1024*1024

# max ratio of ranks to use
max_ratio = 0.6

def parse_excluded_ranks(excluded):
  """Parse a comma-separated list of ranks into a set of ints."""
  if not excluded:
    return set()

  ranks = set()
  for token in excluded.split(","):
    token = token.strip()
    if not token:
      continue
    try:
      ranks.add(int(token))
    except ValueError as exc:
      raise ValueError(f"Invalid rank '{token}' in --exclude-ranks") from exc
  return ranks

def load_json(fname): 
  with open(fname, "rb") as f:
    return json.loads(f.read())

def dmg_storage_query(hosts):
  """Run dmg storage query usage --json"""
  host_list = ",".join(hosts)
  result = subprocess.run(
    ["sudo", "dmg", "storage", "query", "usage", f"-l={host_list}", "--json"],
    capture_output=True,
    text=True,
    check=True
  )
  return json.loads(result.stdout)

def dmg_system_query():
  """Run dmg system query --json"""
  result = subprocess.run(
    ["sudo", "dmg", "system", "query", "--json"],
    capture_output=True,
    text=True,
    check=True
  )
  return json.loads(result.stdout)

#
# Collect all of the ranks into their drgaon fly groups
#

def build_groups(excluded_ranks):
  global max_ranks
  groups = collections.defaultdict(list)

  system_ranks = {}
  if (test):
    system_ranks = load_json("rank.json")
    nvmes = load_json("nvme.json")
  else:
    system_ranks = dmg_system_query()
    hosts = set()
    for item in system_ranks["response"]["members"]:
      hosts.add(item["fault_domain"][1:])
    nvmes = dmg_storage_query(hosts)

  max_ranks = len(system_ranks["response"]["members"])
  rank_avbytes = [0] * max_ranks
  rank_usbytes = [0] * max_ranks
  
  # Exclude any non-joined ranks and all ranks that share their fault domains.
  excluded_fault_domains = set()
  final_excluded_ranks = set(excluded_ranks)
  for item in system_ranks["response"]["members"]:
    rank = int(item["rank"])
    fault_domain = item["fault_domain"]
    if item["state"] != "joined":
      excluded_fault_domains.add(fault_domain)
      final_excluded_ranks.add(rank)

  for item in system_ranks["response"]["members"]:
    rank = int(item["rank"])
    if item["fault_domain"] in excluded_fault_domains:
      final_excluded_ranks.add(rank)

  sorted_excluded_ranks = sorted(final_excluded_ranks)
  print("excluded_ranks: {r}".format(r=",".join(map(str, sorted_excluded_ranks))))

  # get free space
  nvme_data = nvmes["response"]["HostStorage"]

  for server in nvme_data.values():
    if (server["storage"]["nvme_devices"] == None):
      continue
    for item in server["storage"]["nvme_devices"]:
      rank = int(item["smd_devices"][0]["rank"])
      avbytes = int(item["smd_devices"][0]["avail_bytes"])
      usbytes = int(item["smd_devices"][0]["total_bytes"]) - avbytes
      rank_avbytes[rank] += avbytes
      rank_usbytes[rank] += usbytes

  # bin servers into dfly groups
  for item in system_ranks["response"]["members"]:
      rank = int(item["rank"])
      host = item["fault_domain"]
      item["avbytes"] = rank_avbytes[rank]
      item["usbytes"] = rank_usbytes[rank]
      hostnum = int(host.split('-')[2]) - 1
      groupnum = int(hostnum / aurora_server_to_group)
      if (item["state"] == "joined") and rank not in excluded_ranks:
        groups[groupnum].append(item)

  # sort each group
  for g in groups:
    groups[g].sort(key=lambda x: x["avbytes"])

  # print
#  for item in system_ranks["response"]["members"]:
#      print("rank: {r} used: {u} free: {a}".format(r=item["rank"], u=item["usbytes"], a=item["avbytes"]))

  return groups


#
# Create list of ranks based on the algorithm above
#
def select_ranks(groups, size_bytes):
  global max_ranks
  selected_ranks = set()
  target_ranks = min(int(size_bytes / min_bytes_per_rank), int(max_ranks * max_ratio))

  if not groups or target_ranks <= 0:
    return []

  total_candidates = sum(len(items) for items in groups.values())
  target_ranks = min(target_ranks, total_candidates)
  target_ranks = max(target_ranks, 80) # don't allow less than 80 ranks

  # Build fault-domain mapping from currently eligible members.
  domain_to_ranks = collections.defaultdict(set)
  for members in groups.values():
    for item in members:
      domain_to_ranks[item["fault_domain"]].add(int(item["rank"]))

  group_ids = sorted(groups.keys())
  if not group_ids:
    return []

  def remove_rank_from_group_lists(rank):
    for group_id in group_ids:
      members = groups[group_id]
      for idx, member in enumerate(members):
        if int(member["rank"]) == rank:
          members.pop(idx)
          return

  # print ranks in each group
  # for g in groups:
  #     for h in groups[g]:
  #         print(h["rank"], end=",")
  #     print("")

  # select size
  group_index = random.randint(0, len(group_ids)-1)
  while len(selected_ranks) < target_ranks:
    loops = 0
    while loops < len(group_ids) and not groups[group_ids[group_index]]:
      group_index = (group_index + 1) % len(group_ids)
      loops += 1
    if loops == len(group_ids):
      break

    group_id = group_ids[group_index]
    item = groups[group_id].pop()
    rank = int(item["rank"])
    domain = item["fault_domain"]

    # If one rank is chosen from a fault domain, include all eligible ranks from that domain.
    for domain_rank in domain_to_ranks.get(domain, set()):
      if domain_rank in selected_ranks:
        continue
      selected_ranks.add(domain_rank)
      if domain_rank != rank:
        remove_rank_from_group_lists(domain_rank)

    group_index = (group_index + 1) % len(group_ids)

  if len(selected_ranks) > target_ranks:
    print("selection_note: selected more than target to preserve fault-domain rank pairing")

  return list(selected_ranks)

#
# Output the `daos pool create` command
#
def gen_create(label, user, group, size, ranks):
  sorted_ranks = sorted(ranks)
  if sorted_ranks:
      per_rank_tib = size / len(sorted_ranks)
      print("summary: selected_ranks={n} requested_size={s}TiB per_rank={p:.3f}TiB".format(
          n=len(sorted_ranks), s=str(size), p=per_rank_tib
      ))
  else:
      print("summary: selected_ranks=0 requested_size={s}TiB per_rank=0.000TiB".format(s=str(size)))
  print("dmg pool create --properties=rd_fac:3,space_rb:8 --user={u} --group={g} --size={s}T --ranks=\"{rl}\" {l}".format(s=str(size), l=label, u=user, g=group, rl=",".join(map(str, sorted_ranks))))
  
def main():
  parser = argparse.ArgumentParser(description="DAOS Pool Balancer")
  parser.add_argument("--pool", required=True, help="Pool name")
  parser.add_argument("--user", required=True, help="Pool owner")
  parser.add_argument("--group", required=True, help="Primary unix group")
  parser.add_argument("--size", required=True, type=float, help="Pool size in TB")
  parser.add_argument(
    "--exclude-ranks",
    default="",
    help="Comma-separated list of ranks to exclude from selection"
  )

  args = parser.parse_args()
  excluded_ranks = parse_excluded_ranks(args.exclude_ranks)

  size_bytes = int(args.size * 2**40)

  g = build_groups(excluded_ranks)
  ranks = select_ranks(g, size_bytes)
  gen_create(args.pool, args.user, args.group, args.size, ranks)

if __name__ == "__main__":
    main()
