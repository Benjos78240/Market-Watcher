#!/bin/bash
source /home/ufcbu/market-watcher/.venv/bin/activate
nohup uvicorn scripts.api_server:app --host 0.0.0.0 --port 8000 &
echo $! > /home/ufcbu/market-watcher/logs/api_server.pid
echo "API FastAPI lancée en arrière-plan sur le port 8000. PID: $(cat /home/ufcbu/market-watcher/logs/api_server.pid)"
