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

metric_line_re = re.compile(
    r'(?P<key>[^{]+)\{'
    r'(pool="(?P<pool>[a-zA-Z0-9_\-]+)",)?'
    r'rank="(?P<rank>\d+)"'
    r'(,size="(?P<size>\w+)")?'
    r'(,target="(?P<target>\d+)")?'
    r'\}\s+(?P<value>[-+]?\d*\.?\d+(e[+-]\d+)?)'
)

pool_strings =["engine_pool_ops_cont_open"
]

latency_strings =["engine_io_latency_tgt_update_samples",
    "engine_io_latency_tgt_update_sum",
    "engine_io_latency_fetch_samples",
    "engine_io_latency_fetch_sum"
]

def parse_metrics(text, timestamp):

    rows = []

    for line in text.splitlines():

        if line.startswith('#') or not line.strip():
            continue
#        print(line)
        m = metric_line_re.match(line)

        if not m:
            continue

        filled_target = int(m.group("target")) if m.group("target") else -1
        filled_size = m.group("size") if m.group("size") else "NA"
        filled_pool = m.group("pool") if m.group("pool") else "NA"

        if ((m.group("key") in pool_strings) or ((m.group("key") in latency_strings) and ((m.group("size") == "128KB") or (m.group("size") == "1MB")))):
            rows.append({
                "timestamp": timestamp,
                "key": m.group("key"),
                "pool": filled_pool,
                "rank": int(m.group("rank")),
                "target": filled_target,
                "size": filled_size,
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

    parser.add_argument("--ranks")

    parser.add_argument("--interval", type=int, default=0)

    parser.add_argument("--previous_metrics",
        type=str,
        default=None,   # value if not provided
        help="Optional previous metrics filename"
    )

    parser.add_argument("--current_metrics",
        type=str,
        default=None,   # value if not provided
        help="Optional current metrics filename"
    )

    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

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
#    servers = [servers.pop()]
    print("Number of servers =", len(servers))

    pools = fetch_pool_ids()

    df = pd.DataFrame()

    timestamp_string = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    while True:

        if args.current_metrics:
            df = pd.read_pickle(args.current_metrics)
#            df.to_csv('currentpkl.csv',index=False) 
        else:
            print("collecting telemetry... ", end='')
            s=datetime.datetime.now()
            df = collect_once(servers, port)
            e=datetime.datetime.now()
            print("time: {0}".format(e-s))
            df['pool'] = df['pool'].map(pools).fillna(df['pool'])
            base_name = "metrics"
            pkl_filename = f"{base_name}_{timestamp_string}.pkl"
            df.to_pickle(pkl_filename)

        base_name = "report"
        report_filename = f"{base_name}_{timestamp_string}.csv"

        prev_df = pd.DataFrame()

        if args.previous_metrics:
            prev_df = pd.read_pickle(args.previous_metrics)
#            prev_df.to_csv('previouspkl.csv',index=False) 
            merge_df = pd.merge(df,
                prev_df,
                on=["key","pool","rank", "target", "size"],
                how="left",
                suffixes=("_current","_previous")
            )  

            merge_df["delta"] = merge_df["value_current"].fillna(0) - merge_df["value_previous"].fillna(0) 
            merge_df = merge_df.rename(columns={'delta': 'value'})          
            merge_df = merge_df.drop(columns=["timestamp_current", "timestamp_previous"])
            df = merge_df
#            for row in df.itertuples():
#            if row.value > 0.0:
#                print(row)
        df_filtered = df[df['target'] != -1]

        pivot_df = df_filtered.pivot(
            index=['rank','target','size'],
            columns='key',
            values='value'
        )
        pivot_df['target_update_avg'] = np.where(pivot_df['engine_io_latency_tgt_update_samples'] == 0.0,0,pivot_df['engine_io_latency_tgt_update_sum'] / pivot_df['engine_io_latency_tgt_update_samples'])
        pivot_df['fetch_avg'] = np.where(pivot_df['engine_io_latency_fetch_samples'] == 0.0,0,pivot_df['engine_io_latency_fetch_sum'] / pivot_df['engine_io_latency_fetch_samples'])
#        print(pivot_df.to_string())
#       print(pivot_df.sort_values(by='target_update_avg', ascending=False).head(20).to_string())

#        print("median value:")
#        row = pivot_df.loc[(pivot_df['target_update_avg'] - pivot_df['target_update_avg'].median()).abs().idxmin()].to_string()
#        print(row)
# Flatten multi-level columns
        pivot_df.columns = [
            '_'.join(map(str, col)).strip()
            if isinstance(col, tuple) else col
            for col in pivot_df.columns
        ]

# Move index into columns
        pivot_df = pivot_df.reset_index()

        pivot_df.sort_values(by='target_update_avg', ascending=False).to_csv(report_filename,index=False)
#        for row in df.itertuples():
#            if row.value > 0.0:
#                print(row)

        if args.interval == 0:
            break

        time.sleep(args.interval)


if __name__ == "__main__":
    main()
