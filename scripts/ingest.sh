#!/bin/bash

# ==========================================
# AgriFlow Ingestion Script
# Description: Moves valid files from Raw to Staging
# Author: Kabelo Modimoeng
# ==========================================

# 1. Configuration (Relative Paths)
RAW_DIR="data/raw"
STAGING_DIR="data/staging"
LOG_FILE="logs/ingestion.log"

# Get current timestamp for logging
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

# Function to write to log file
log() {
    echo "[$TIMESTAMP] $1" | tee -a "$LOG_FILE"
}

log "🚀 Starting Ingestion Job..."

# 2. Safety Check: Do the directories exist?
mkdir -p "$STAGING_DIR"
mkdir -p "logs"

# 3. Check if Raw Directory is empty
if [ -z "$(ls -A $RAW_DIR)" ]; then
    log "⚠️  No files found in $RAW_DIR. Sleeping..."
    exit 0
fi

# 4. Loop through files in Raw
count=0
for file in "$RAW_DIR"/*; do
    # Check if file exists (handles edge case of empty loop)
    [ -e "$file" ] || continue
    
    filename=$(basename "$file")

    # Filter for CSV and JSON only
    if [[ "$filename" == *.csv ]] || [[ "$filename" == *.json ]]; then
        
        # Validation: Is the file empty? (0 bytes)
        if [ ! -s "$file" ]; then
            log "❌ ERROR: $filename is empty (0 bytes). Skipping."
            continue
        fi

        # Move to Staging
        mv "$file" "$STAGING_DIR/"
        log "✅ Moved $filename to Staging Area."
        ((count++))
    else
        log "ℹ️  Ignoring non-data file: $filename"
    fi
done

log "🏁 Job Complete. Processed $count files."
log "----------------------------------------"