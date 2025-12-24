#!/bin/bash
echo "🛑 正在停止舊的進程..."
sudo fuser -k 8080/tcp
echo "🚀 正在啟動 Sideship-Killer 極限模式..."
go run cmd/main.go
