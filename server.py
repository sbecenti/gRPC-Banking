import grpc 
from concurrent import futures
import banks_pb2_grpc 
from branch import Branch
from multiprocessing import Process
import json
import sys
import os
import time

def serve(branch_id, balance, branches):
    # create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # create a Branch instance
    branch = Branch(branch_id, balance, branches)        

    # register the branch with the gRPC server
    banks_pb2_grpc.add_RPCServicer_to_server(branch, server)

    # create server port based on the branch ID
    port = 10000 + branch_id
    server.add_insecure_port(f"localhost:{port}")

    # start server and wait for termination
    server.start()
    server.wait_for_termination()

def start_branches(filename):

    # read the branch data from the JSON file
    with open(filename, "r") as file:
        data = json.load(file)
    branches = [item for item in data if item["type"] == "branch"]

    print("Number of branches: ", len(branches))

    # create a list of branch ids to pass to each branch process
    branch_ids = [branch["id"] for branch in branches]

    # if the signal file exists, remove it to indicate that the branches are not yet complete
    signal_file = "banking_complete.flag"
    if os.path.exists(signal_file):
        os.remove(signal_file)

    # start a separate process for each branch
    print(f"Starting {len(branches)} branch processes...")
    processes = []
    for branch in branches:
        p = Process(target=serve, args=(branch["id"], branch["balance"], branch_ids))
        p.start()
        processes.append(p)

    print("running branch processes: ", sum(p.is_alive() for p in processes))

    # check for signal file to terminate the branch processes
    try:
        while not os.path.exists(signal_file):
            time.sleep(1)

        print("Signal file detected. Terminating branch processes...")

    finally:
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join()
        print(f"All {len(branches)} branch processes terminated.")

        # remove the signal file after all branch processes have been terminated
        if os.path.exists(signal_file):
            os.remove(signal_file)

if __name__ == "__main__":
    start_branches(sys.argv[1])

