import json
from customer import Customer
import sys
from multiprocessing import Process, Queue
from collections import defaultdict
import grpc
import banks_pb2
import banks_pb2_grpc

# Function to start multiple customer processes based on the JSON file
# Args:
#     filename (str): The path to the JSON file containing customer data.
def start_customers(filename):
    with open(filename, "r") as file:
        data = json.load(file)
    
    customers = [item for item in data if item["type"] == "customer"]
    branches = [item for item in data if item["type"] == "branch"]
    
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

    # add branch event results
    for branch in branches:
        branch_result = get_branch_events(branch["id"])
        results.append(branch_result)

    # group events by customer request ID
    events_by_request = defaultdict(list)
    for process in results:
        for event in process["events"]:
            request_id = event["customer-request-id"]
            events_by_request[request_id].append({
                "id": process["id"],
                "customer-request-id": request_id,
                "type": process["type"],
                "logical_clock": event["logical_clock"],
                "interface": event["interface"],
                "comment": event["comment"]
            })

    # sort and append grouped events
    for request_id in sorted(events_by_request):
        request_events = events_by_request[request_id]
        
        # order by time
        request_events.sort(key=lambda event: event["logical_clock"])

        #add to output
        results.extend(request_events)
        

    # write the results to output.json
    with open("output.json", "w") as file:
        json.dump(results, file, indent=4)
    print("Results written to output.json")

    # signal to server that all customers have completed their operations
    with open("banking_complete.flag", "w") as file:
        file.write("Banking operations completed.")

# Function to run a single customer process
# Args:
#     customer_data (dict): The data of the customer.
#     result_queue (Queue): The queue to store the results of the customer process.
def run_customer(customer_data, result_queue):
    c = Customer(customer_data["id"], customer_data["events"])
    c.createStub()
    c.executeEvents()

    # append message to results
    result_queue.put({
        "id": c.id,
        "type": "customer",
        "events": c.event_log
    })

# Function to get branch events
# Args:
#     branch_id (int): The ID of the branch.
# Returns:
#     list: The list of events for the branch.
def get_branch_events(branch_id):
    # connect
    port = 10000 + branch_id

    with grpc.insecure_channel(f"localhost:{port}") as channel:
        stub = banks_pb2_grpc.RPCStub(channel)

        # request event history
        request = banks_pb2.Request(interface="get_events")
        response = stub.MsgDelivery(request)

        # print(f"Branch {branch_id} response: {response}")
        # print(f"Events JSON: {repr(response.events_json)}")

        # turn to python list
        events = json.loads(response.events_json)

        return {
            "id": branch_id,
            "type": "branch",
            "events": events
        }
    

if __name__ == "__main__":
    start_customers(sys.argv[1])