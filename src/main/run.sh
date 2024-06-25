#!/usr/bin/env bash

go build -buildmode=plugin ../mrapps/wc.go

rm -rf mr-tmp
mkdir mr-tmp

rm output/* -rf

go run mrcoordinator.go input/pg-*.txt
