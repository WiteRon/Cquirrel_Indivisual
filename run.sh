#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

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

# Step 2: Create data directory
echo -e "${YELLOW}[2/7] Preparing data directory...${NC}"
mkdir -p scripts/data
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
START_TIME=$(date +%s)

# Start the Java process in background and monitor progress
java -jar target/stream-query-engine-1.0.0.jar > output.txt 2>&1 &
JAVA_PID=$!

# Monitor progress with detailed information
PREV_LINES=0
PREV_SIZE=0
PREV_PROCESSED=0
LAST_UPDATE=0
while kill -0 $JAVA_PID 2>/dev/null; do
    sleep 1
    ELAPSED=$(( $(date +%s) - START_TIME ))
    
    if [ -f output.txt ]; then
        CURRENT_LINES=$(wc -l < output.txt 2>/dev/null | tr -d ' \n' || echo 0)
        CURRENT_LINES=${CURRENT_LINES:-0}
        
        # Estimate processed records - use multiple methods
        # Method 1: Count "Routed data" messages (each represents one input record processed)
        ROUTED_COUNT=$(grep -c "Routed data" output.txt 2>/dev/null | tr -d ' \n' || echo 0)
        ROUTED_COUNT=$((ROUTED_COUNT + 0))
        
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
        
        # Check for Flink job status
        JOB_STARTED=$(grep -c "Stream Query Processing Engine" output.txt 2>/dev/null | tr -d ' \n' || echo 0)
        JOB_STARTED=${JOB_STARTED:-0}
        JOB_STARTED=$((JOB_STARTED + 0))
        if [ "$JOB_STARTED" -gt 0 ]; then
            JOB_STATUS="Running"
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
    echo -e "  ${GREEN}✓ Execution completed (took ${DURATION}s)${NC}"
else
    echo -e "  ${RED}✗ Execution failed! Check output.txt for details${NC}"
    echo "  → Last 20 lines of output.txt:"
    tail -20 output.txt
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

cd ..

if [ $VALIDATION_EXIT -eq 0 ]; then
    echo -e "  ${GREEN}✓ Validation completed (took ${VALIDATION_DURATION}s)${NC}"
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
    
    echo "=== Execution Summary ==="
    echo "  • Execution log: output.txt"
    echo "  • Validation log: validation.txt"
    echo "  • Output file: scripts/data/output.csv"
    echo ""
    echo -e "${YELLOW}⚠ Check validation.txt for details${NC}"
fi
