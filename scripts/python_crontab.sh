#!/bin/bash
echo "Lancement à $(date)" >> /home/ufcbu/market-watcher/logs/which_python_cron.log
which python3 >> /home/ufcbu/market-watcher/logs/which_python_cron.log
python3 --version >> /home/ufcbu/market-watcher/logs/which_python_cron.log
python3 -m pip list >> /home/ufcbu/market-watcher/logs/which_python_cron.log
echo "----" >> /home/ufcbu/market-watcher/logs/which_python_cron.log