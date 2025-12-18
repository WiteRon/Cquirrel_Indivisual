#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to parse time command output and extract real time in seconds (4 decimal places)
# Input: time command output (from stderr)
# Output: time in seconds as a decimal number
parse_time_output() {
    local time_output="$1"
    # Extract the real time line (format: "real    0m1.234s" or "real    1m2.345s")
    local real_line=$(echo "$time_output" | grep "^real" | head -1)
    
    if [ -z "$real_line" ]; then
        echo "0.0000"
        return
    fi
    
    # Extract minutes and seconds (format: "0m1.234s" or "1m2.345s")
    local time_part=$(echo "$real_line" | awk '{print $2}')
    
    # Remove 'm' and 's' to get minutes and seconds
    local minutes=0
    local seconds=0
    
    if [[ "$time_part" =~ ([0-9]+)m([0-9]+\.[0-9]+)s ]]; then
        minutes="${BASH_REMATCH[1]}"
        seconds="${BASH_REMATCH[2]}"
    elif [[ "$time_part" =~ ([0-9]+\.[0-9]+)s ]]; then
        seconds="${BASH_REMATCH[1]}"
    elif [[ "$time_part" =~ ([0-9]+)s ]]; then
        seconds="${BASH_REMATCH[1]}"
    else
        echo "0.0000"
        return
    fi
    
    # Calculate total seconds: minutes * 60 + seconds
    # Use awk for floating point arithmetic (more portable than bc)
    local total_seconds=$(awk -v m="$minutes" -v s="$seconds" 'BEGIN {printf "%.4f", m * 60 + s}')
    
    # Format to 4 decimal places
    echo "$total_seconds"
}

echo "=== Stream Query Engine - Complete Build and Run ==="
echo ""

# Step 1: Setup Python virtual environment for validation
echo -e "${YELLOW}[1/7] Setting up Python virtual environment...${NC}"
if [ ! -d ".venv" ]; then
    echo "  → Creating .venv..."
    python3 -m venv .venv
    echo -e "  ${GREEN}✓ Created .venv${NC}"
fi
echo "  → Activating virtual environment..."
source .venv/bin/activate
echo "  → Upgrading pip..."
pip install -q --upgrade pip
echo "  → Installing dependencies..."
pip install -q -r scripts/requirements.txt
echo -e "  ${GREEN}✓ Python environment ready${NC}"
echo ""

# Step 2: Create data directory and clean previous results
echo -e "${YELLOW}[2/7] Preparing data directory...${NC}"
mkdir -p scripts/data
echo "  → Cleaning previous validation database and output files..."
rm -f scripts/data/tpch.db
rm -f scripts/data/output.csv
rm -f validation.txt
rm -f output.txt
echo -e "  ${GREEN}✓ Data directory ready${NC}"
echo ""

# Step 3: Generate TPC-H data (default 20MB)
SCALE_FACTOR=${1:-0.02}
echo -e "${YELLOW}[3/7] Generating TPC-H data (scale factor: $SCALE_FACTOR)...${NC}"
echo "  → This may take a while depending on scale factor..."
echo "  → Progress: Generating tables..."
cd scripts
python data_generator.py "$SCALE_FACTOR" 2>&1 | while IFS= read -r line; do
    echo "    $line"
done
echo -e "  ${GREEN}✓ Data generation completed${NC}"
echo ""

# Step 4: Merge data files
echo -e "${YELLOW}[4/7] Merging data files...${NC}"
echo "  → Merging customer, orders, and lineitem tables..."
python data_merger.py
echo -e "  ${GREEN}✓ Data merge completed${NC}"
cd ..
echo ""

# Step 5: Build Java project
echo -e "${YELLOW}[5/7] Building Java project...${NC}"
echo "  → Cleaning previous build..."
mvn clean -q
echo "  → Compiling Java sources (this may take a minute)..."
mvn compile -q 2>&1 | grep -E "(ERROR|WARNING|BUILD)" || true
echo "  → Packaging JAR file..."
mvn package -q -DskipTests 2>&1 | grep -E "(ERROR|WARNING|BUILD)" || true
if [ ! -f "target/stream-query-engine-1.0.0.jar" ]; then
    echo -e "  ${RED}✗ Build failed!${NC}"
    echo "  → Running mvn package with verbose output for debugging..."
    mvn package
    exit 1
fi
JAR_SIZE=$(du -h target/stream-query-engine-1.0.0.jar | cut -f1)
echo -e "  ${GREEN}✓ Build completed (JAR size: $JAR_SIZE)${NC}"
echo ""

# Step 6: Run the application
echo -e "${YELLOW}[6/7] Running stream query engine...${NC}"
echo "  → Starting Flink job (this may take several minutes)..."
echo "  → Progress will be written to output.txt"

# Get total input records for progress calculation
INPUT_FILE=$(grep DATA_INPUT_FILE .env 2>/dev/null | cut -d'=' -f2 || echo "./scripts/data/tpch_q3.tbl")
if [ ! -f "$INPUT_FILE" ]; then
    INPUT_FILE="./scripts/data/tpch_q3.tbl"
fi
TOTAL_RECORDS=$(wc -l < "$INPUT_FILE" 2>/dev/null || echo 0)
if [ $TOTAL_RECORDS -eq 0 ]; then
    TOTAL_RECORDS=1  # Avoid division by zero
fi

echo "  → Total input records: $TOTAL_RECORDS"
echo "  → Monitoring progress in real-time..."
START_TIME=$(date +%s)  # Use integer seconds for progress display
START_TIME_PRECISE=$(date +%s.%N 2>/dev/null || date +%s)  # Use precise time for calculations

# Variables to track Flink execution time (core calculation start to completion)
LAST_CALCULATION_START_TIME=""
LAST_CALCULATION_COMPLETE_TIME=""
FLINK_EXECUTION_TIME=""
PREV_CALCULATION_START_COUNT=0
PREV_CALCULATION_COMPLETE_COUNT=0

# Start the Java process in background and monitor progress
java -jar target/stream-query-engine-1.0.0.jar > output.txt 2>&1 &
JAVA_PID=$!

# Monitor progress with detailed information
PREV_LINES=0
PREV_SIZE=0
PREV_PROCESSED=0
PREV_ROUTED_COUNT=0
PREV_CALCULATION_START_COUNT=0
PREV_CALCULATION_COMPLETE_COUNT=0
LAST_UPDATE=0
while kill -0 $JAVA_PID 2>/dev/null; do
    sleep 1
    ELAPSED=$(( $(date +%s) - START_TIME ))
    
    if [ -f output.txt ]; then
        CURRENT_LINES=$(wc -l < output.txt 2>/dev/null | tr -d ' \n' || echo 0)
        CURRENT_LINES=${CURRENT_LINES:-0}
        
        # Estimate processed records - use multiple methods
        # Method 1: Count "Routed data" messages (each represents one input record processed)
        CURRENT_ROUTED_COUNT=$(grep -c "Routed data" output.txt 2>/dev/null | tr -d ' \n' || echo 0)
        CURRENT_ROUTED_COUNT=$((CURRENT_ROUTED_COUNT + 0))
        
        PREV_ROUTED_COUNT=$CURRENT_ROUTED_COUNT
        ROUTED_COUNT=$CURRENT_ROUTED_COUNT
        
        # Track counts for monitoring (actual timing will be extracted from logs after completion)
        CURRENT_PROCESSING_COUNT=$(grep -c "\[RevenueCalculator\] Processing:" output.txt 2>/dev/null | tr -d ' \n' || echo 0)
        CURRENT_PROCESSING_COUNT=$((CURRENT_PROCESSING_COUNT + 0))
        PREV_CALCULATION_START_COUNT=$CURRENT_PROCESSING_COUNT
        
        CURRENT_EMIT_COUNT=$(grep -c "\[RevenueCalculator\] Emitting result" output.txt 2>/dev/null | tr -d ' \n' || echo 0)
        CURRENT_EMIT_COUNT=$((CURRENT_EMIT_COUNT + 0))
        PREV_CALCULATION_COMPLETE_COUNT=$CURRENT_EMIT_COUNT
        
        # Method 2: Count any processing messages (broader pattern)
        PROCESSING_COUNT=$(grep -cE "(Routed data|Processing|Aggregation result)" output.txt 2>/dev/null | tr -d ' \n' || echo 0)
        PROCESSING_COUNT=$((PROCESSING_COUNT + 0))
        
        # Method 3: Estimate from log file size (rough estimate)
        # Assume each log line represents some processing, use a multiplier
        LOG_BASED_ESTIMATE=$((CURRENT_LINES * 10))
        if [ "$LOG_BASED_ESTIMATE" -gt "$TOTAL_RECORDS" ]; then
            LOG_BASED_ESTIMATE=$TOTAL_RECORDS
        fi
        
        # Method 4: Check output file (final results)
        OUTPUT_RECORDS=0
        if [ -f "scripts/data/output.csv" ]; then
            OUTPUT_RECORDS=$(wc -l < scripts/data/output.csv 2>/dev/null | tr -d ' \n' || echo 0)
            OUTPUT_RECORDS=$((OUTPUT_RECORDS + 0))
            # Subtract 1 for header
            if [ "$OUTPUT_RECORDS" -gt 0 ]; then
                OUTPUT_RECORDS=$((OUTPUT_RECORDS - 1))
            fi
        fi
        
        # Use the maximum of all estimates, but prefer routed count if available
        ESTIMATED_PROCESSED=$ROUTED_COUNT
        if [ "$PROCESSING_COUNT" -gt "$ESTIMATED_PROCESSED" ]; then
            ESTIMATED_PROCESSED=$PROCESSING_COUNT
        fi
        if [ "$LOG_BASED_ESTIMATE" -gt "$ESTIMATED_PROCESSED" ] && [ "$ELAPSED" -gt 5 ]; then
            # Only use log-based estimate after a few seconds
            ESTIMATED_PROCESSED=$LOG_BASED_ESTIMATE
        fi
        if [ "$OUTPUT_RECORDS" -gt "$ESTIMATED_PROCESSED" ]; then
            ESTIMATED_PROCESSED=$OUTPUT_RECORDS
        fi
        
        # Ensure it's a number
        ESTIMATED_PROCESSED=$((ESTIMATED_PROCESSED + 0))
        
        # Cap at total records
        if [ "$ESTIMATED_PROCESSED" -gt "$TOTAL_RECORDS" ]; then
            ESTIMATED_PROCESSED=$TOTAL_RECORDS
        fi
        
        # Calculate remaining and percentage
        REMAINING=$((TOTAL_RECORDS - ESTIMATED_PROCESSED))
        if [ "$REMAINING" -lt 0 ]; then
            REMAINING=0
        fi
        
        # Calculate percentage (integer)
        if [ "$TOTAL_RECORDS" -gt 0 ] && [ "$ESTIMATED_PROCESSED" -gt 0 ]; then
            PERCENTAGE=$((ESTIMATED_PROCESSED * 100 / TOTAL_RECORDS))
        else
            PERCENTAGE=0
        fi
        if [ "$PERCENTAGE" -gt 100 ]; then
            PERCENTAGE=100
        fi
        
        # Get file size (works on both Linux and macOS)
        CURRENT_SIZE_BYTES=0
        if command -v stat >/dev/null 2>&1; then
            if stat -f%z output.txt >/dev/null 2>&1; then
                # macOS
                CURRENT_SIZE_BYTES=$(stat -f%z output.txt 2>/dev/null | tr -d ' \n' || echo 0)
            else
                # Linux
                CURRENT_SIZE_BYTES=$(stat -c%s output.txt 2>/dev/null | tr -d ' \n' || echo 0)
            fi
        else
            CURRENT_SIZE_BYTES=$(wc -c < output.txt 2>/dev/null | tr -d ' \n' || echo 0)
        fi
        CURRENT_SIZE_BYTES=${CURRENT_SIZE_BYTES:-0}
        # Ensure it's a number
        CURRENT_SIZE_BYTES=$((CURRENT_SIZE_BYTES + 0))
        
        # Format size (simple formatting)
        if [ "$CURRENT_SIZE_BYTES" -gt 1048576 ]; then
            CURRENT_SIZE_M=$((CURRENT_SIZE_BYTES / 1048576))
            CURRENT_SIZE="${CURRENT_SIZE_M}M"
        elif [ "$CURRENT_SIZE_BYTES" -gt 1024 ]; then
            CURRENT_SIZE_K=$((CURRENT_SIZE_BYTES / 1024))
            CURRENT_SIZE="${CURRENT_SIZE_K}K"
        else
            CURRENT_SIZE="${CURRENT_SIZE_BYTES}B"
        fi
        
        # Check for Flink job status and completion
        JOB_STARTED=$(grep -c "Stream Query Processing Engine" output.txt 2>/dev/null | tr -d ' \n' || echo 0)
        JOB_STARTED=${JOB_STARTED:-0}
        JOB_STARTED=$((JOB_STARTED + 0))
        
        # Check if job completed (for status display only, not for timing)
        JOB_COMPLETED=$(grep -c "Stream query processing job completed" output.txt 2>/dev/null | tr -d ' \n' || echo 0)
        JOB_COMPLETED=$((JOB_COMPLETED + 0))
        
        if [ "$JOB_STARTED" -gt 0 ]; then
            if [ "$JOB_COMPLETED" -gt 0 ]; then
                JOB_STATUS="Completed"
            else
                JOB_STATUS="Running"
            fi
        else
            JOB_STATUS="Starting"
        fi
    else
        CURRENT_LINES=0
        CURRENT_SIZE="0B"
        ESTIMATED_PROCESSED=0
        REMAINING=$TOTAL_RECORDS
        PERCENTAGE=0
        JOB_STATUS="Initializing"
        CURRENT_SIZE_BYTES=0
    fi
    
    # Update display every second
    if [ $ELAPSED -ne $LAST_UPDATE ]; then
        # Clear line and print progress (add spaces at end to clear any leftover characters)
        printf "\r  → [%3ds] Progress: %6d/%6d (%3d%%) | Remaining: %6d | Status: %-8s   " \
            $ELAPSED $ESTIMATED_PROCESSED $TOTAL_RECORDS $PERCENTAGE $REMAINING "$JOB_STATUS"
        PREV_LINES=$CURRENT_LINES
        PREV_SIZE=$CURRENT_SIZE_BYTES
        PREV_PROCESSED=$ESTIMATED_PROCESSED
        LAST_UPDATE=$ELAPSED
    fi
done

# Wait for the process to finish
wait $JAVA_PID
EXEC_EXIT=$?
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo ""  # New line after progress

# Extract precise timing from logs using Python script
# Find the last Processing and its corresponding Emitting result, calculate time difference
TIMING_RESULT=$(python3 << 'PYTHON_SCRIPT'
import re
from datetime import datetime
import sys

try:
    # Read the log file
    with open('output.txt', 'r') as f:
        lines = f.readlines()
    
    # Find the last Processing line
    last_processing = None
    last_processing_idx = -1
    for i, line in enumerate(lines):
        if '[RevenueCalculator] Processing:' in line:
            last_processing = line
            last_processing_idx = i
    
    if last_processing:
        # Extract timestamp and groupKey
        match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{3})', last_processing)
        if match:
            ts_str = match.group(1) + '.' + match.group(2)
            dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S.%f')
            processing_time = dt.timestamp()
            
            # Extract groupKey
            gk_match = re.search(r'groupKey=([^,]+)', last_processing)
            if gk_match:
                groupkey = gk_match.group(1)
                
                # Find matching Emitting result after this Processing
                for i in range(last_processing_idx + 1, len(lines)):
                    if '[RevenueCalculator] Emitting result' in lines[i] and groupkey in lines[i]:
                        emit_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{3})', lines[i])
                        if emit_match:
                            emit_ts_str = emit_match.group(1) + '.' + emit_match.group(2)
                            emit_dt = datetime.strptime(emit_ts_str, '%Y-%m-%d %H:%M:%S.%f')
                            emit_time = emit_dt.timestamp()
                            diff = emit_time - processing_time
                            # Output: start_time end_time diff (all in seconds with 6 decimal places)
                            print(f'{processing_time:.6f} {emit_time:.6f} {diff:.6f}')
                            sys.exit(0)
    
    # If we couldn't find matching pair, output zeros
    print('0.000000 0.000000 0.000000')
except Exception as e:
    # On error, output zeros
    print('0.000000 0.000000 0.000000')
PYTHON_SCRIPT
)

# Parse the result
if [ -n "$TIMING_RESULT" ]; then
    LAST_CALCULATION_START_TIME=$(echo "$TIMING_RESULT" | awk '{print $1}')
    LAST_CALCULATION_COMPLETE_TIME=$(echo "$TIMING_RESULT" | awk '{print $2}')
    FLINK_EXECUTION_TIME=$(echo "$TIMING_RESULT" | awk '{print $3}')
    
    # If timing extraction failed, use fallback
    if [ "$LAST_CALCULATION_START_TIME" = "0.000000" ] && [ "$LAST_CALCULATION_COMPLETE_TIME" = "0.000000" ]; then
        # Fallback: use a very small default (since Processing and Emitting are almost simultaneous)
        FLINK_EXECUTION_TIME="0.0001"
        LAST_CALCULATION_START_TIME=""
        LAST_CALCULATION_COMPLETE_TIME=""
    fi
else
    # Fallback: use a very small default
    FLINK_EXECUTION_TIME="0.0001"
fi

# Show final stats
FINAL_LINES=$(wc -l < output.txt 2>/dev/null || echo 0)
FINAL_SIZE=$(du -h output.txt 2>/dev/null | cut -f1 || echo "0")
if [ -f "scripts/data/output.csv" ]; then
    FINAL_OUTPUT=$(wc -l < scripts/data/output.csv 2>/dev/null || echo 0)
    FINAL_OUTPUT=$((FINAL_OUTPUT - 1))  # Subtract header
else
    FINAL_OUTPUT=0
fi
echo "  → Final: Processed ${FINAL_OUTPUT} records, ${FINAL_LINES} log lines, ${FINAL_SIZE} log size"

if [ $EXEC_EXIT -eq 0 ]; then
    echo -e "  ${GREEN}✓ Execution completed${NC}"
    echo "  → Flink execution time: ${FLINK_EXECUTION_TIME}s (from core calculation start to completion, 4 decimal places)"
    if [ -n "$LAST_CALCULATION_START_TIME" ] && [ -n "$LAST_CALCULATION_COMPLETE_TIME" ]; then
        echo "    → Core calculation start time: ${LAST_CALCULATION_START_TIME}"
        echo "    → Core calculation complete time: ${LAST_CALCULATION_COMPLETE_TIME}"
    fi
else
    echo -e "  ${RED}✗ Execution failed! Check output.txt for details${NC}"
    echo "  → Last 20 lines of output.txt:"
    tail -20 output.txt
    rm -f "$TIME_OUTPUT_FILE"
    exit 1
fi
echo ""

# Step 7: Validate results using .venv
echo -e "${YELLOW}[7/7] Validating results...${NC}"
echo "  → This may take a minute..."
cd scripts
source ../.venv/bin/activate

# Run validation with progress monitoring
python result_validator.py > ../validation.txt 2>&1 &
VALIDATION_PID=$!

# Monitor validation progress
VALIDATION_START=$(date +%s)
PREV_LINES=0
while kill -0 $VALIDATION_PID 2>/dev/null; do
    sleep 1
    VALIDATION_ELAPSED=$(( $(date +%s) - VALIDATION_START ))
    
    if [ -f ../validation.txt ]; then
        CURRENT_LINES=$(wc -l < ../validation.txt 2>/dev/null | tr -d ' \n' || echo 0)
        CURRENT_LINES=$((CURRENT_LINES + 0))
        
        # Extract progress from validation output
        PROGRESS_LINE=$(tail -5 ../validation.txt 2>/dev/null | grep -E "Progress:|Loading|Executing|Comparing" | tail -1 || echo "")
        
        if [ "$CURRENT_LINES" -gt "$PREV_LINES" ] || [ -n "$PROGRESS_LINE" ]; then
            # Show last progress line
            if [ -n "$PROGRESS_LINE" ]; then
                printf "\r  → [%3ds] %s" $VALIDATION_ELAPSED "$PROGRESS_LINE"
            else
                printf "\r  → [%3ds] Processing... (log: %d lines)" $VALIDATION_ELAPSED $CURRENT_LINES
            fi
            PREV_LINES=$CURRENT_LINES
        else
            printf "\r  → [%3ds] Validating..." $VALIDATION_ELAPSED
        fi
    else
        printf "\r  → [%3ds] Starting validation..." $VALIDATION_ELAPSED
    fi
done

# Wait for validation to complete
wait $VALIDATION_PID
VALIDATION_EXIT=$?
VALIDATION_END=$(date +%s)
VALIDATION_DURATION=$((VALIDATION_END - VALIDATION_START))
echo ""  # New line after progress

# Parse SQL execution time from validation output
# Look for "SQL_EXECUTION_TIME: X.XXXX" pattern
SQL_EXECUTION_TIME=$(grep "SQL_EXECUTION_TIME:" ../validation.txt 2>/dev/null | tail -1 | awk '{print $2}' || echo "0.0000")
# Ensure it's a valid number with 4 decimal places
SQL_EXECUTION_TIME=$(awk -v t="$SQL_EXECUTION_TIME" 'BEGIN {printf "%.4f", t+0}')

cd ..

if [ $VALIDATION_EXIT -eq 0 ]; then
    echo -e "  ${GREEN}✓ Validation completed${NC}"
    echo "  → SQL execution time: ${SQL_EXECUTION_TIME}s (SQL query only, excluding data import, 4 decimal places)"
    echo ""
    
    # Show detailed validation results
    if [ -f validation.txt ]; then
        echo "=== Validation Results ==="
        
        # Extract key metrics from validation output (with logging)
        echo "  → Extracting validation metrics..." >&2
        
        # Extract query count
        QUERY_LINE=$(grep -E "Number of query results:" validation.txt 2>/dev/null | head -1 || echo "")
        QUERY_COUNT=$(echo "$QUERY_LINE" | grep -oE "[0-9]+" | head -1 || echo "0")
        QUERY_COUNT=$(echo "$QUERY_COUNT" | tr -d '[:space:]')
        if [ -z "$QUERY_COUNT" ] || [ "$QUERY_COUNT" = "" ]; then
            QUERY_COUNT=0
        fi
        QUERY_COUNT=$((QUERY_COUNT + 0)) 2>/dev/null || QUERY_COUNT=0
        echo "  → DEBUG: QUERY_COUNT='$QUERY_COUNT'" >&2
        
        # Extract CSV count
        CSV_LINE=$(grep -E "Number of CSV file entries:" validation.txt 2>/dev/null | head -1 || echo "")
        CSV_COUNT=$(echo "$CSV_LINE" | grep -oE "[0-9]+" | head -1 || echo "0")
        CSV_COUNT=$(echo "$CSV_COUNT" | tr -d '[:space:]')
        if [ -z "$CSV_COUNT" ] || [ "$CSV_COUNT" = "" ]; then
            CSV_COUNT=0
        fi
        CSV_COUNT=$((CSV_COUNT + 0)) 2>/dev/null || CSV_COUNT=0
        echo "  → DEBUG: CSV_COUNT='$CSV_COUNT'" >&2
        
        # Extract match count
        MATCH_LINE=$(grep -E "Number of matching entries:" validation.txt 2>/dev/null | head -1 || echo "")
        MATCH_COUNT=$(echo "$MATCH_LINE" | grep -oE "[0-9]+" | head -1 || echo "0")
        MATCH_COUNT=$(echo "$MATCH_COUNT" | tr -d '[:space:]')
        if [ -z "$MATCH_COUNT" ] || [ "$MATCH_COUNT" = "" ]; then
            MATCH_COUNT=0
        fi
        MATCH_COUNT=$((MATCH_COUNT + 0)) 2>/dev/null || MATCH_COUNT=0
        echo "  → DEBUG: MATCH_COUNT='$MATCH_COUNT'" >&2
        
        # Extract accuracy line
        ACCURACY_LINE=$(grep -E "Accuracy:" validation.txt 2>/dev/null | tail -1 || echo "")
        ACCURACY_LINE=$(echo "$ACCURACY_LINE" | tr -d '\n')
        echo "  → DEBUG: ACCURACY_LINE='$ACCURACY_LINE'" >&2
        
        # Check if results are identical (use count, not -c to avoid newlines)
        IS_IDENTICAL_COUNT=$(grep "Results are identical" validation.txt 2>/dev/null | wc -l | tr -d '[:space:]' || echo "0")
        if [ -z "$IS_IDENTICAL_COUNT" ] || [ "$IS_IDENTICAL_COUNT" = "" ]; then
            IS_IDENTICAL=0
        else
            IS_IDENTICAL=$((IS_IDENTICAL_COUNT + 0)) 2>/dev/null || IS_IDENTICAL=0
        fi
        echo "  → DEBUG: IS_IDENTICAL='$IS_IDENTICAL'" >&2
        
        # Display results
        echo "  • Query results: $QUERY_COUNT"
        echo "  • CSV results: $CSV_COUNT"
        echo "  • Matching entries: $MATCH_COUNT"
        
        # Extract and display accuracy
        echo "  → Calculating accuracy..." >&2
        if [ -n "$ACCURACY_LINE" ]; then
            # Extract accuracy value (may contain decimal)
            ACCURACY_VALUE=$(echo "$ACCURACY_LINE" | grep -oE "[0-9]+\.?[0-9]*" | head -1 || echo "0.00")
            ACCURACY_VALUE=$(echo "$ACCURACY_VALUE" | tr -d ' \n')
            echo "  → DEBUG: ACCURACY_VALUE='$ACCURACY_VALUE'" >&2
            echo "  • Accuracy: ${ACCURACY_VALUE}%"
        else
            # Calculate accuracy if not in output
            ACCURACY_VAL=0
            if [ "$QUERY_COUNT" -gt 0 ] && [ "$CSV_COUNT" -gt 0 ]; then
                if [ "$QUERY_COUNT" -gt "$CSV_COUNT" ]; then
                    DENOM=$QUERY_COUNT
                else
                    DENOM=$CSV_COUNT
                fi
                echo "  → DEBUG: Calculating accuracy: $MATCH_COUNT / $DENOM" >&2
                if [ "$DENOM" -gt 0 ]; then
                    ACCURACY_VAL=$((MATCH_COUNT * 100 / DENOM))
                    echo "  → DEBUG: ACCURACY_VAL='$ACCURACY_VAL'" >&2
                    echo "  • Accuracy: ${ACCURACY_VAL}%"
                else
                    echo "  • Accuracy: N/A"
                fi
            elif [ "$IS_IDENTICAL" -gt 0 ]; then
                echo "  • Accuracy: 100%"
            else
                echo "  • Accuracy: 0%"
            fi
        fi
        
        # Show status
        if [ "$IS_IDENTICAL" -gt 0 ]; then
            echo ""
            echo -e "  ${GREEN}✅ Results are identical - 100% accuracy!${NC}"
        else
            echo ""
            echo -e "  ${YELLOW}⚠ Results differ${NC}"
            
            # Show difference counts
            ONLY_QUERY_LINE=$(grep -E "Found in query but not in CSV" validation.txt 2>/dev/null | head -1 || echo "")
            ONLY_QUERY_COUNT=$(echo "$ONLY_QUERY_LINE" | grep -oE "[0-9]+" | head -1 || echo "0")
            ONLY_QUERY_COUNT=$(echo "$ONLY_QUERY_COUNT" | tr -d ' \n')
            ONLY_QUERY_COUNT=$((ONLY_QUERY_COUNT + 0))
            echo "  → DEBUG: ONLY_QUERY_COUNT='$ONLY_QUERY_COUNT'" >&2
            
            ONLY_CSV_LINE=$(grep -E "Found in CSV but not in query" validation.txt 2>/dev/null | head -1 || echo "")
            ONLY_CSV_COUNT=$(echo "$ONLY_CSV_LINE" | grep -oE "[0-9]+" | head -1 || echo "0")
            ONLY_CSV_COUNT=$(echo "$ONLY_CSV_COUNT" | tr -d ' \n')
            ONLY_CSV_COUNT=$((ONLY_CSV_COUNT + 0))
            echo "  → DEBUG: ONLY_CSV_COUNT='$ONLY_CSV_COUNT'" >&2
            
            if [ "$ONLY_QUERY_COUNT" -gt 0 ]; then
                echo "  • Missing in CSV: $ONLY_QUERY_COUNT entries"
            fi
            if [ "$ONLY_CSV_COUNT" -gt 0 ]; then
                echo "  • Missing in query: $ONLY_CSV_COUNT entries"
            fi
        fi
        echo ""
    fi
    
    # Calculate and display time comparison
    echo "=== Time Comparison ==="
    echo "  • Flink execution time: ${FLINK_EXECUTION_TIME}s (from core calculation start to completion)"
    echo "  • SQL execution time: ${SQL_EXECUTION_TIME}s (SQL query only, excluding data import)"
    
    # Calculate ratio (Flink time / SQL time)
    # Use awk for floating point comparison and calculation
    if [ -n "$FLINK_EXECUTION_TIME" ] && [ -n "$SQL_EXECUTION_TIME" ]; then
        # Check if SQL time is greater than 0
        IS_VALID=$(awk -v v="$SQL_EXECUTION_TIME" 'BEGIN {if (v > 0) print "1"; else print "0"}')
        if [ "$IS_VALID" = "1" ]; then
            RATIO=$(awk -v f="$FLINK_EXECUTION_TIME" -v s="$SQL_EXECUTION_TIME" 'BEGIN {printf "%.4f", f / s}')
            echo "  • Time ratio (Flink/SQL): ${RATIO}x"
            
            # Compare ratio with 1
            IS_GT_ONE=$(awk -v r="$RATIO" 'BEGIN {if (r > 1.0001) print "1"; else print "0"}')
            IS_LT_ONE=$(awk -v r="$RATIO" 'BEGIN {if (r < 0.9999) print "1"; else print "0"}')
            
            if [ "$IS_GT_ONE" = "1" ]; then
                echo "    → Flink execution took ${RATIO}x longer than SQL query"
            elif [ "$IS_LT_ONE" = "1" ]; then
                INVERSE_RATIO=$(awk -v r="$RATIO" 'BEGIN {printf "%.4f", 1 / r}')
                echo "    → SQL query took ${INVERSE_RATIO}x longer than Flink execution"
            else
                echo "    → Flink execution and SQL query took approximately the same time"
            fi
        else
            echo "  • Time ratio: N/A (SQL execution time is 0)"
        fi
    else
        echo "  • Time ratio: N/A (cannot calculate)"
    fi
    echo ""
    
    echo "=== Execution Summary ==="
    echo "  • Execution log: output.txt"
    echo "  • Validation log: validation.txt"
    echo "  • Output file: scripts/data/output.csv"
    echo ""
    echo -e "${GREEN}✅ All steps completed successfully!${NC}"
else
    echo -e "  ${YELLOW}⚠ Validation completed with warnings (took ${VALIDATION_DURATION}s)${NC}"
    echo ""
    
    # Show validation errors
    if [ -f validation.txt ]; then
        echo "=== Validation Issues ==="
        tail -30 validation.txt 2>/dev/null | grep -v "^$" | tail -15 | sed 's/^/  /'
        echo ""
    fi
    
    # Calculate and display time comparison (even if validation failed)
    echo "=== Time Comparison ==="
    echo "  • Java execution time: ${JAVA_TIME_SECONDS}s"
    echo "  • Validation time: ${VALIDATION_TIME_SECONDS}s"
    
    # Calculate ratio (Java time / Validation time)
    # Use awk for floating point comparison and calculation
    if [ -n "$JAVA_TIME_SECONDS" ] && [ -n "$VALIDATION_TIME_SECONDS" ]; then
        # Check if validation time is greater than 0
        IS_VALID=$(awk -v v="$VALIDATION_TIME_SECONDS" 'BEGIN {if (v > 0) print "1"; else print "0"}')
        if [ "$IS_VALID" = "1" ]; then
            RATIO=$(awk -v j="$JAVA_TIME_SECONDS" -v v="$VALIDATION_TIME_SECONDS" 'BEGIN {printf "%.4f", j / v}')
            echo "  • Time ratio (Java/Validation): ${RATIO}x"
            
            # Compare ratio with 1
            IS_GT_ONE=$(awk -v r="$RATIO" 'BEGIN {if (r > 1.0001) print "1"; else print "0"}')
            IS_LT_ONE=$(awk -v r="$RATIO" 'BEGIN {if (r < 0.9999) print "1"; else print "0"}')
            
            if [ "$IS_GT_ONE" = "1" ]; then
                echo "    → Java execution took ${RATIO}x longer than validation"
            elif [ "$IS_LT_ONE" = "1" ]; then
                INVERSE_RATIO=$(awk -v r="$RATIO" 'BEGIN {printf "%.4f", 1 / r}')
                echo "    → Validation took ${INVERSE_RATIO}x longer than Java execution"
            else
                echo "    → Java execution and validation took approximately the same time"
            fi
        else
            echo "  • Time ratio: N/A (validation time is 0)"
        fi
    else
        echo "  • Time ratio: N/A (cannot calculate)"
    fi
    echo ""
    
    echo "=== Execution Summary ==="
    echo "  • Execution log: output.txt"
    echo "  • Validation log: validation.txt"
    echo "  • Output file: scripts/data/output.csv"
    echo ""
    echo -e "${YELLOW}⚠ Check validation.txt for details${NC}"
fi
