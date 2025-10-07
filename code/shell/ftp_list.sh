#!/bin/bash

# Configuration
LOGGER_HOST='192.168.3.100'
USERNAME='CCFC'
PASSWORD='EPCN'
REMOTE_DIR="CRD"            # Card directory on the logger
OUTPUT_FILE='/home/imchugh/Documents/logger_filelist.txt'
SAFE_FILELIST="/home/imchugh/Documents/logger_safe_files.txt"

# Get raw FTP directory listing
ftp -inv $LOGGER_HOST <<EOF > $OUTPUT_FILE
user $USERNAME $PASSWORD
cd $REMOTE_DIR
ls
bye
EOF

# Extract file names from listing
# Assumes standard UNIX-style `ls` output from logger
awk '{print $NF}' "$OUTPUT_FILE" | grep -v '\.dat$' > "$SAFE_FILELIST"

echo "Full listing saved to $OUTPUT_FILE"
echo "Filtered list (excluding open .dat file) saved to $SAFE_FILELIST"

