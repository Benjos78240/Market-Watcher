#!/bin/bash
# This script is used to run the extraction of historical data from Coinbase
# It will run the Python script in the background and log the output
export LD_LIBRARY_PATH=/home/ufcbu/oracle/instantclient/instantclient_23_8
export TNS_ADMIN=/home/ufcbu/wallet_dbmarketwatcher
cd /home/ufcbu/market-watcher
nohup python3 -m scripts.extraction_coinbase_histo_