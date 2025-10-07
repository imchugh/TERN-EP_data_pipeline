#!/bin/bash

# Configuration
LOGGER_HOST='192.168.3.100'
USERNAME='CCFC'
PASSWORD='EPCN'
REMOTE_DIR="CRD"            # Card directory on the logger
REMOTE_FILE="TOB3_Calperum_EC.fast_data_1.dat"     # The file you want to copy
LOCAL_FILE="/home/imchugh/Documents/Calperum_EC.TERNflux.dat"      # Local name for the copy

# Fetch the file
ftp -inv $LOGGER_HOST <<EOF
user $USERNAME $PASSWORD
cd $REMOTE_DIR
get $REMOTE_FILE $LOCAL_FILE
bye
EOF

echo "File '$REMOTE_FILE' copied from logger to local as '$LOCAL_FILE'."

