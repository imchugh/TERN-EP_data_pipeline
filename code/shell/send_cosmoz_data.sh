#!/bin/bash

# Configuration
LOCAL_FILE="/store/Raw_data/$1/Ancillary/$1_cosmoz_CRNS.dat" # Local file to be sent
REMOTE_USER="cosmoz_station"
REMOTE_HOST="pftp.csiro.au" 
REMOTE_DIR="/incoming/$2" # Destination directory

# Check if a file was provided
if [ -z "$LOCAL_FILE" ]; then
    echo "Usage: $0 <local_file>"
    exit 1
fi

# Check if the local file exists
if [ ! -f "$LOCAL_FILE" ]; then
    echo "Error: File '$LOCAL_FILE' does not exist."
    exit 1
fi

# Send file via SFTP
sftp -i ~/.ssh/cosmoz_station_key "${REMOTE_USER}@${REMOTE_HOST}" <<EOF
put "${LOCAL_FILE}" "${REMOTE_DIR}/"
bye
EOF

# Check if the SFTP command succeeded
if [ $? -eq 0 ]; then
    echo "File '$LOCAL_FILE' sent successfully to '${REMOTE_DIR}' on '${REMOTE_HOST}'."
else
    echo "Error sending file."
fi
