import random


def read_lines(file_name, limit=None):
    with open(file_name, 'r') as f:
        lines = f.readlines()
    return lines[:limit] if limit else lines


def generate_q3_input():
    # Read the original TPC-H table data
    customer_lines = read_lines('./data/customer.tbl')
    orders_lines = read_lines('./data/orders.tbl')
    lineitem_lines = read_lines('./data/lineitem.tbl')

    # Generate input data with operation symbols (INSERT/DELETE)
    output = []
    for line in customer_lines:
        output.append(f"INSERT|customer|{line.strip()}")
    for line in orders_lines:
        output.append(f"INSERT|orders|{line.strip()}")
    for line in lineitem_lines:
        output.append(f"INSERT|lineitem|{line.strip()}")

    # Randomly insert some DELETE operations (to simulate data changes)
    random.shuffle(output)
    for i in range(1000):  # Randomly delete 1000 records
        idx = random.randint(0, len(output)-1)
        output[idx] = output[idx].replace("INSERT", "DELETE")

    # Write to the output file
    with open('./data/tpch_q3.tbl', 'w') as f:
        f.write('\n'.join(output))


if __name__ == '__main__':
    generate_q3_input()
    print("Q3 input data generated: tpch_q3.tbl")

