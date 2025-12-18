import duckdb
import os
import csv
from decimal import Decimal, ROUND_HALF_UP
import time

# db file directory
DATABASE_PATH = "data/tpch.db"


def process_tpch_q3(tbl_path, query_path, output_csv_path, db_path=DATABASE_PATH):
    """
    execute TPCH Q3 query using db file
    :param tbl_path: tpch_q3.tbl file path
    :param query_path: SQL query file path
    :param output_csv_path: expected output CSV file path
    :param db_path: DuckDB db file path
    """
    import sys
    
    # ensure data directory existed
    os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else '.', exist_ok=True)

    # check the db existed
    db_exists = os.path.isfile(db_path)

    # connect db file
    print("  → Connecting to DuckDB...", flush=True)
    con = duckdb.connect(database=db_path, read_only=False)

    try:
        if not db_exists:
            print("  → Creating database and loading data...", flush=True)
            create_tables(con)
            total_lines = sum(1 for _ in open(tbl_path, 'r')) if os.path.exists(tbl_path) else 0
            print(f"  → Loading {total_lines} records from {tbl_path}...", flush=True)
            load_tpch_q3_data(con, tbl_path, total_lines)
            print("  → Data loaded and database saved to disk.", flush=True)
        else:
            print("  → Database found, using existing data.", flush=True)

        # execute query
        print("  → Executing SQL query...", flush=True)
        query_result = execute_query(con, query_path)

        # read the expected result
        print("  → Reading output CSV file...", flush=True)
        csv_result = read_output_csv(output_csv_path)

        # compare the results between query and csv
        print("  → Comparing results...", flush=True)
        compare_results(query_result, csv_result)

    except Exception as e:
        print(f"  ✗ Execution error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        con.close()


def create_tables(con):
    """Create the TPCH table structure"""
    con.execute("""
        CREATE TABLE customer (
            c_custkey    INT,
            c_name       VARCHAR,
            c_address    VARCHAR,
            c_nationkey  INT,
            c_phone      VARCHAR,
            c_acctbal    DECIMAL(12, 2),
            c_mktsegment VARCHAR,
            c_comment    VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE orders (
            o_orderkey      INT,
            o_custkey       INT,
            o_orderstatus   CHAR(1),
            o_totalprice    DECIMAL(12, 2),
            o_orderdate     DATE,
            o_orderpriority VARCHAR,
            o_clerk         VARCHAR,
            o_shippriority  INT,
            o_comment       VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE lineitem (
            l_orderkey      INT,
            l_partkey       INT,
            l_suppkey       INT,
            l_linenumber    INT,
            l_quantity      DECIMAL(12, 2),
            l_extendedprice DECIMAL(12, 2),
            l_discount      DECIMAL(12, 2),
            l_tax           DECIMAL(12, 2),
            l_returnflag    CHAR(1),
            l_linestatus    CHAR(1),
            l_shipdate      DATE,
            l_commitdate    DATE,
            l_receiptdate   DATE,
            l_shipinstruct  VARCHAR,
            l_shipmode      VARCHAR,
            l_comment       VARCHAR
        )
    """)
    print("Table structure created.")


def load_tpch_q3_data(con, tbl_path, total_lines=0):
    """Read tpch_q3.tbl and process INSERT/DELETE operations"""
    if not os.path.exists(tbl_path):
        raise FileNotFoundError(f"File {tbl_path} does not exist")

    table_field_counts = {
        "customer": 8,
        "orders": 9,
        "lineitem": 16
    }

    # Count total lines if not provided
    if total_lines == 0:
        print("    Counting total lines...", flush=True)
        with open(tbl_path, 'r') as f:
            total_lines = sum(1 for _ in f)
        print(f"    Total lines: {total_lines}", flush=True)

    processed = 0
    last_progress_time = time.time()
    batch_size = 1000  # Process in batches for better performance
    
    with open(tbl_path, 'r') as f:
        batch = []
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            parts = line.split('|')
            if len(parts) < 3:
                continue

            operation = parts[0].upper()
            table_name = parts[1].lower()

            if table_name not in table_field_counts:
                continue

            fields = parts[2:]
            if len(fields) != table_field_counts[table_name]:
                continue

            if operation == "INSERT":
                placeholders = ', '.join(['?' for _ in fields])
                batch.append((f"INSERT INTO {table_name} VALUES ({placeholders})", fields))
                processed += 1

            elif operation == "DELETE":
                if table_name == "customer":
                    batch.append(("DELETE FROM customer WHERE c_custkey = ?", [fields[0]]))
                elif table_name == "orders":
                    batch.append(("DELETE FROM orders WHERE o_orderkey = ?", [fields[0]]))
                elif table_name == "lineitem":
                    batch.append(("DELETE FROM lineitem WHERE l_orderkey = ? AND l_linenumber = ?", [fields[0], fields[3]]))
                processed += 1
            
            # Execute batch and show progress
            if len(batch) >= batch_size:
                for query, params in batch:
                    con.execute(query, params)
                batch = []
                
                # Progress update every second or every 5000 records
                current_time = time.time()
                if current_time - last_progress_time >= 1.0 or line_num % 5000 == 0:
                    progress = (line_num * 100) // total_lines if total_lines > 0 else 0
                    print(f"    Progress: {line_num}/{total_lines} ({progress}%) | Processed: {processed} records", flush=True)
                    last_progress_time = current_time

    # Execute remaining batch
    if batch:
        for query, params in batch:
            con.execute(query, params)
    
    # Final progress update
    if total_lines > 0:
        print(f"    Progress: {total_lines}/{total_lines} (100%) | Processed: {processed} records", flush=True)

    print(f"  → Data loaded: {processed} records processed from .tbl file.", flush=True)


def execute_query(con, query_path):
    """Execute the query and return formatted results"""
    if not os.path.exists(query_path):
        raise FileNotFoundError(f"Query file {query_path} does not exist")

    with open(query_path, 'r') as f:
        query = f.read()

    begin_time = time.perf_counter()
    print(f"    Query execution started...", flush=True)

    result = con.execute(query).fetchall()

    end_time = time.perf_counter()
    time_cost = end_time - begin_time

    print(f"    Query execution completed in {time_cost:.3f} seconds", flush=True)
    print(f"    Found {len(result)} result rows", flush=True)

    # Format results
    formatted = []
    for row in result:
        orderkey = row[0]
        orderdate = str(row[1])  # Convert to 'YYYY-MM-DD' string
        shippriority = row[2]
        revenue = Decimal(str(row[3])).quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP)
        formatted.append((orderkey, orderdate, shippriority, revenue))

    return set(formatted)  # Use a set for unordered comparison


def read_output_csv(csv_path):
    """Read output.csv and return formatted results"""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Output file {csv_path} does not exist")

    formatted = []
    total_lines = sum(1 for _ in open(csv_path, 'r')) - 1  # Subtract header
    print(f"    Reading {total_lines} records from CSV...", flush=True)
    
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header

        for line_num, row in enumerate(reader, 2):  # Line numbers start from 2 (header is 1)
            if not row:
                continue

            # Handle possible spaces (e.g., "48899, 1995-01-19, 0, 13272.0672")
            cleaned = [col.strip() for col in row]
            if len(cleaned) != 4:
                if line_num % 1000 == 0:
                    print(f"    Warning: Line {line_num} in CSV has incorrect format, skipping", flush=True)
                continue

            try:
                orderkey = int(cleaned[0])
                orderdate = cleaned[1]
                shippriority = int(cleaned[2])
                revenue = Decimal(cleaned[3]).quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP)
                formatted.append((orderkey, orderdate, shippriority, revenue))
            except (ValueError, Decimal.InvalidOperation) as e:
                if line_num % 1000 == 0:
                    print(f"    Warning: Data conversion failed for line {line_num}, Error: {e}", flush=True)
            
            # Progress update every 1000 records
            if line_num % 1000 == 0:
                progress = ((line_num - 1) * 100) // total_lines if total_lines > 0 else 0
                print(f"    Progress: {line_num - 1}/{total_lines} ({progress}%)", flush=True)

    print(f"    Loaded {len(formatted)} valid records from CSV", flush=True)
    return set(formatted)


def compare_results(query_set, csv_set):
    """Compare query results with CSV results"""
    only_query = query_set - csv_set
    only_csv = csv_set - query_set
    matching = query_set & csv_set

    print("\n===== Result Comparison =====", flush=True)
    print(f"Number of query results: {len(query_set)}", flush=True)
    print(f"Number of CSV file entries: {len(csv_set)}", flush=True)
    print(f"Number of matching entries: {len(matching)}", flush=True)
    
    # Calculate accuracy
    if len(query_set) > 0 and len(csv_set) > 0:
        # Use the larger set as denominator for accuracy
        if len(query_set) >= len(csv_set):
            denominator = len(query_set)
        else:
            denominator = len(csv_set)
        accuracy = (len(matching) * 100.0) / denominator if denominator > 0 else 0.0
        print(f"Accuracy: {accuracy:.2f}%", flush=True)
    elif len(query_set) == 0 and len(csv_set) > 0:
        print("Accuracy: 0.00% (Query returned no results)", flush=True)
    elif len(csv_set) == 0 and len(query_set) > 0:
        print("Accuracy: 0.00% (CSV file is empty)", flush=True)
    else:
        print("Accuracy: N/A (Both sets are empty)", flush=True)

    if not only_query and not only_csv:
        print("\n✅ Results are identical - 100% accuracy!", flush=True)
    else:
        print("\n⚠ Results differ:", flush=True)
        if only_query:
            print(f"  • Found in query but not in CSV: {len(only_query)} entries", flush=True)
            if len(only_query) <= 10:
                for item in sorted(only_query):
                    print(f"    {item}", flush=True)
            else:
                for item in sorted(list(only_query)[:5]):
                    print(f"    {item}", flush=True)
                print(f"    ... and {len(only_query) - 5} more", flush=True)
        if only_csv:
            print(f"  • Found in CSV but not in query: {len(only_csv)} entries", flush=True)
            if len(only_csv) <= 10:
                for item in sorted(only_csv):
                    print(f"    {item}", flush=True)
            else:
                for item in sorted(list(only_csv)[:5]):
                    print(f"    {item}", flush=True)
                print(f"    ... and {len(only_csv) - 5} more", flush=True)


if __name__ == "__main__":
    # 配置路径
    TPCH_Q3_TBL_PATH = "./data/tpch_q3.tbl"
    QUERY_FILE = "./query3.sql"
    OUTPUT_CSV_PATH = "./data/output.csv"

    process_tpch_q3(TPCH_Q3_TBL_PATH, QUERY_FILE, OUTPUT_CSV_PATH)

