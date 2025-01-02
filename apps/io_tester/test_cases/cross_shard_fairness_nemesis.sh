#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 IO_TESTER_EXECUTABLE" >&2
    exit 1
fi

exec "$1" --smp=7 --storage=/dev/null --conf=<(cat <<'EOF'
- name: tablet-streaming
  data_size: 1GB
  shards: all
  type: seqread
  shard_info:
    parallelism: 50
    reqsize: 128kB
    shares: 200
- name: cassandra-stress
  shards: all
  type: randread
  data_size: 1GB
  shard_info:
    parallelism: 100
    reqsize: 1536
    shares: 1000
    rps: 75
  options:
    pause_distribution: poisson
    sleep_type: steady
- name: cassandra-stress-slight-imbalance
  shards: [0]
  type: randread
  data_size: 1GB
  shard_info:
    parallelism: 100
    reqsize: 1536
    class: cassandra-stress
    rps: 10
  options:
    pause_distribution: poisson
    sleep_type: steady
EOF
) --io-properties-file=<(cat <<'EOF'
# i4i.2xlarge
disks:
- mountpoint: /dev
  read_bandwidth: 1542559872
  read_iops: 218786
  write_bandwidth: 1130867072
  write_iops: 121499
EOF
)
