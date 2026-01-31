#!/bin/bash
export LD_LIBRARY_PATH=/home/ufcbu/oracle/instantclient/instantclient_23_8
export TNS_ADMIN=/home/ufcbu/wallet_dbmarketwatcher
cd /home/ufcbu/market-watcher/
/usr/bin/python3 -m scripts.user_watchlist_alert