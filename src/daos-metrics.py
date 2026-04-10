#!/usr/bin/env python3

import datetime
import json
import time
import re
import argparse
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def fetch_pool_ids():
    import subprocess
    pools = {}
    cmd = ["daos", "pool", "list", "-j"]
    proc = subprocess.run(cmd, capture_output=True)
    j = json.loads(proc.stdout)
    for item in j['response']['pools']:
        pools[item['uuid']] = item['label']
    return pools

# ------------------------------------------------------------
# Metric parsing
# ------------------------------------------------------------

    #r'(?P<key>[^{]+)\{rank="(?P<rank>\d+)",target="(?P<target>\d+)"\}\s+(?P<value>[-+]?\d*\.?\d+)'
metric_line_re = re.compile(
    r'(?P<key>[^{]+)\{pool="(?P<pool>[a-zA-Z0-9_\-]+)",rank="(?P<rank>\d+)"\}\s+(?P<value>[-+]?\d*\.?\d+)'
)

def parse_metrics(text, timestamp):

    rows = []

    for line in text.splitlines():

        if line.startswith('#') or not line.strip():
            continue

        m = metric_line_re.match(line)

        if not m:
            continue

        rows.append({
            "timestamp": timestamp,
            "key": m.group("key"),
            "rank": int(m.group("rank")),
            "pool": m.group("pool"),
            #"target": int(m.group("target")),
            "value": float(m.group("value"))
        })

    return rows


# ------------------------------------------------------------
# TCP metric retrieval
# ------------------------------------------------------------

def fetch_metrics(server, port):
    import urllib.request

    try:
        data = urllib.request.urlopen("http://{server}:{port}/metrics".format(server=server,port=port)).read().decode('utf-8')
        ts = datetime.datetime.utcnow()
        return parse_metrics(data, ts)
    except Exception as e:
        print("server failed = ", server, "with", e)
        return {"timestamp": datetime.datetime.utcnow()}



# ------------------------------------------------------------
# Rank list parser
# ------------------------------------------------------------

def parse_rank_list(expr):

    ranks = []

    expr = expr.strip("[]")

    for part in expr.split(","):

        if "-" in part:
            a, b = part.split("-")
            ranks.extend(range(int(a), int(b) + 1))
        else:
            ranks.append(int(part))

    return ranks


# ------------------------------------------------------------
# Deviation detection
# ------------------------------------------------------------

def detect_outliers(df, metric):

    subset = df[df.key == metric]

    pivot = subset.pivot_table(
        values="value",
        index="timestamp",
        columns="rank",
        aggfunc="mean"
    )

    zscores = (pivot - pivot.mean()) / pivot.std()

    outliers = abs(zscores) > 3

    return pivot, outliers


# ------------------------------------------------------------
# Graph specific metric
# ------------------------------------------------------------

def plot_metric(df, metric, ranks):

    subset = df[(df['key'] == metric) & (df['rank'].isin(ranks))]

    pivot = subset.pivot_table(
        values="value",
        index="timestamp",
        columns="rank",
        aggfunc="mean"
    )
    print(pivot)

    pivot.plot(figsize=(12,6))

    plt.title(f"Metric: {metric}")
    plt.ylabel("Value")
    plt.xlabel("Time")
    plt.legend(title="Rank")

    plt.show()


# ------------------------------------------------------------
# Deviation graph
# ------------------------------------------------------------

def plot_outliers(pivot, outliers):

    plt.figure(figsize=(12,6))

    for rank in pivot.columns:

        if outliers[rank].any():
            plt.plot(pivot.index, pivot[rank], label=f"Rank {rank} *")

        else:
            plt.plot(pivot.index, pivot[rank], alpha=0.3)

    plt.title("Deviation Detection")
    plt.ylabel("Metric Value")
    plt.xlabel("Time")

    plt.legend()

    plt.show()


# ------------------------------------------------------------
# Metric collection
# ------------------------------------------------------------

def collect_once(servers, port):

    rows = []

    with ThreadPoolExecutor() as pool:

        futures = [
            pool.submit(fetch_metrics, s, port)
            for s in servers
        ]

        for f in futures:
            rows.extend(f.result())

    return pd.DataFrame(rows)


# ------------------------------------------------------------
# Main loop
# ------------------------------------------------------------

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--config", required=True)

    parser.add_argument("--metric")
    parser.add_argument("--ranks")

    parser.add_argument("--interval", type=int, default=0)

    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    port = config["Port"]
    server_list = config["servers"]

    servers = []

#    part = server_list[0].split('-', 2)
#    if part[2][0] == '[':
#        ranges = part[2][1:-2].split(',')
#        for r in ranges:
#          v = r.split('-')
#          if len(v) > 1:
#              for n in range(int(v[0]), int(v[1])+1):
#                  servers.append("{0}-{1}-{2:04}".format(part[0], part[1], n))
#          else:
#              servers.append("{0}-{1}-{2:04}".format(part[0], part[1], int(v[0])))
#    else:
#        servers.append("{0}-{1}-{2}".format(part[0], part[1], part[2]))

    f = open("/admin_share/DAOS/usage/daos_user/daos_user.dmg_system_query.2026-04-07.json", "rt")
    j = json.load(f)
    f.close()
    bad = set()
    for s in j['response']['members']:
        if s['state'] == 'joined' and s['addr'] not in bad:
            servers.append(s['addr'].split(':')[0])
        else:
            bad.add(s['addr'])

    servers = set(servers)
    print("server =", len(servers))

    history = []

    pools = fetch_pool_ids()

    while True:

        print("collecting telemetry... ", end='')
        s=datetime.datetime.now()
        df = collect_once(servers, port)
        e=datetime.datetime.now()
        print("time: {0}".format(e-s))

        df['pool'] = df['pool'].map(pools)

        history.append(df)

        all_data = pd.concat(history)

        #for row in df[df['key'] == 'engine_pool_ops_pool_connect'].itertuples():
        #for row in df[df['key'] == 'engine_pool_ops_cont_open'].itertuples():
        for row in df[df['key'] == args.metric].itertuples():
            #if row.value > 0:
                print(row)

        if args.metric and args.ranks:

            ranks = parse_rank_list(args.ranks)

            plot_metric(all_data, args.metric, ranks)

        elif args.metric:

            pivot, outliers = detect_outliers(all_data, args.metric)

            plot_outliers(pivot, outliers)

        if args.interval == 0:
            break

        time.sleep(args.interval)


if __name__ == "__main__":
    main()
