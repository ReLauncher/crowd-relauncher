#!/bin/bash

echo "===================================="
echo "= FIREBASE CROWDFLOWER LOGS TO CSV ="
echo "===================================="

echo "Now we create a folder for $@ task"
mkdir "Datasets/Logs/$@"
echo "Now we generate logs of key presses from firebase..."
node Scripts/key_presses_to_csv.js $@
echo "Now we generate logs of clicks from firebase..."
node Scripts/clicks_to_csv.js $@
echo "Now we generate logs of page activity from firebase..."
node Scripts/page_activity_to_csv.js $@
echo "Now we generate logs of tab visibility from firebase..."
node Scripts/tab_visibility_to_csv.js $@

#echo "\nNow we copy the folder with logs to be further processed"
#cp -r "Logs/$@" "../Datasets/Logs/$@"

echo "\nDONE"
echo "===================================="