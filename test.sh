#!/bin/bash

SCRIPT_DIR=$(pwd)/bin
mkdir -p $SCRIPT_DIR
go build -o $SCRIPT_DIR/mini-etcd
$MAELSTROM_PATH/maelstrom test -w lin-kv --bin $SCRIPT_DIR/mini-etcd --time-limit 10 --node-count 3 --concurrency 2n --rate 10
