#!/bin/bash
source /home/ufcbu/market-watcher/.venv/bin/activate
python3 /home/ufcbu/market-watcher/scripts/utils/log_checker.py "API FastAPI" uvicorn 1
