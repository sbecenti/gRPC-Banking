import json
from customer import Customer
import sys
from multiprocessing import Process, Queue

def start_customers(filename):
    with open(filename, "r") as file:
        data = json.load(file)
    
    customers = [item for item in data if item["type"] == "customer"]

    results = []
    result_queue = Queue()

    print(f"Executing {len(customers)} customer processes...")
    for customer in customers:
        # create a separate process for each customer
        p = Process(target=run_customer, args=(customer, result_queue))
        p.start()

        # get customer results from queue
        results.append(result_queue.get())

        p.join()

        # make sure the process has terminated properly
        if p.is_alive():
            p.terminate()
            p.join()
            raise Exception(f"Customer {customer['id']} process did not terminate properly.")

        if p.exitcode != 0:
            raise Exception(f"Customer {customer['id']} process exited with code {p.exitcode}.")

    print(f"Completed {len(customers)} customer processes.")

    # write the results to output.json
    with open("output.json", "w") as file:
        json.dump(results, file, indent=4)
    print("Results written to output.json")

    # signal to server that all customers have completed their operations
    with open("banking_complete.flag", "w") as file:
        file.write("Banking operations completed.")

# run a customer process
def run_customer(customer_data, result_queue):
    c = Customer(customer_data["id"], customer_data["events"])
    c.createStub()
    c.executeEvents()

    # append message to results
    result_queue.put({
        "id": c.id,
        "recv": c.recvMsg
    })

if __name__ == "__main__":
    start_customers(sys.argv[1])