#!/bin/bash

# ===== Configuration =====
LOGGER_HOST='192.168.3.100'
USERNAME='CCFC'
PASSWORD='EPCN'
REMOTE_DIR="CRD"            # Card directory on the logger
REMOTE_FILE="TOB3_Calperum_EC.fast_data_1.dat"     # The file you want to copy
LOCAL_FILE="/home/imchugh/Documents/TOB3_Calperum_EC.fast_data_1.dat"
LOGFILE="/home/imchugh/Documents/ftp_$(date +%Y%m%d_%H%M%S).log"

MAX_RETRIES=5
RETRY_DELAY=10                       # seconds between retries
TIMEOUT_SECONDS=3600                 # max transfer time per attempt
LIMIT_RATE=100k                      # optional, e.g., 500 KB/s, set to 0 to disable

# ===== Retry loop =====
attempt=1
while [ $attempt -le $MAX_RETRIES ]; do
    echo "Attempt $attempt of $MAX_RETRIES..."

    # Use timeout to avoid hanging indefinitely
    if [ "$LIMIT_RATE" != "0" ]; then
        timeout $TIMEOUT_SECONDS lftp -u "$USERNAME","$PASSWORD" "$LOGGER_HOST" <<EOF | tee "$LOGFILE"
set ftp:passive-mode on
set net:timeout 20
set net:reconnect-interval-base 5
set net:reconnect-interval-multiplier 2.0
set net:limit-rate $LIMIT_RATE
cd $REMOTE_DIR
get -c $REMOTE_FILE -o $LOCAL_FILE
bye
EOF
    else
        timeout $TIMEOUT_SECONDS lftp -u "$USERNAME","$PASSWORD" "$LOGGER_HOST" <<EOF | tee "$LOGFILE"
set ftp:passive-mode on
set net:timeout 20
set net:reconnect-interval-base 5
set net:reconnect-interval-multiplier 2.0
cd $REMOTE_DIR
get -c $REMOTE_FILE -o $LOCAL_FILE
bye
EOF
    fi

    # Check if file exists and is non-empty
    if [ -f "$LOCAL_FILE" ] && [ -s "$LOCAL_FILE" ]; then
        echo "File downloaded successfully: $LOCAL_FILE"
        break
    else
        echo "Transfer failed or timed out. Retrying in $RETRY_DELAY seconds..."
        sleep $RETRY_DELAY
        attempt=$((attempt+1))
    fi
done

if [ $attempt -gt $MAX_RETRIES ]; then
    echo "Failed to download file after $MAX_RETRIES attempts."
fi
