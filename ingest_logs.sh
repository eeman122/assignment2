#!/bin/bash

# Check if a date parameter is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 YYYY-MM-DD"
    exit 1
fi

# Parse year, month, and day
DATE=$1
YEAR=$(echo $DATE | cut -d'-' -f1)
MONTH=$(echo $DATE | cut -d'-' -f2)
DAY=$(echo $DATE | cut -d'-' -f3)

# Define local and HDFS paths
LOG_FILE="$DATE.csv"
METADATA_FILE="content_metadata.csv"
LOCAL_DIR="/home/hadoop/asg2/data"
HDFS_LOG_DIR="/raw/logs/$YEAR/$MONTH/$DAY"
HDFS_METADATA_DIR="/raw/metadata/$YEAR/$MONTH/$DAY"

# Create HDFS directories if they don't exist
hdfs dfs -mkdir -p $HDFS_LOG_DIR
hdfs dfs -mkdir -p $HDFS_METADATA_DIR

# Copy log file to HDFS
if hdfs dfs -put "$LOCAL_DIR/$LOG_FILE" "$HDFS_LOG_DIR/"; then
    echo "Successfully ingested log file: $LOG_FILE to $HDFS_LOG_DIR"
else
    echo "Failed to ingest log file: $LOG_FILE"
fi

# Copy metadata file to HDFS (only once per batch)
if hdfs dfs -test -e "$HDFS_METADATA_DIR/$METADATA_FILE"; then
    echo "Metadata file already exists in HDFS, skipping..."
else
    if hdfs dfs -put "$LOCAL_DIR/$METADATA_FILE" "$HDFS_METADATA_DIR/"; then
        echo "Successfully ingested metadata file: $METADATA_FILE to $HDFS_METADATA_DIR"
    else
        echo "Failed to ingest metadata file: $METADATA_FILE"
    fi
fi

