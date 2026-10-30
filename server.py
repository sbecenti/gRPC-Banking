import grpc 
from concurrent import futures

import banks_pb2_grpc 
from branch import Branch

from multiprocessing import Process

import json

def serve(branch_id, balance, branches):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    branch = Branch(branch_id, balance, branches)        

    # register the branch with the gRPC server
    banks_pb2_grpc.add_RPCServicer_to_server(branch, server)

    # create server port based on the branch ID
    port = 5000 + branch_id
    server.add_insecure_port(f"localhost:{port}")

    # start server and wait for termination
    server.start()
    print(f"Branch {branch_id} started on port {port}")
    server.wait_for_termination()

def start_branches():

    with open("input.json", "r") as files:
        data = json.load(files)
    
    branches = [item for item in data if item["type"] == "branch"]

    print("Number of branches: ", len(branches))

    branch_ids = [branch["id"] for branch in branches]

    # start a separate process for each branch
    processes = []
    for branch in branches:
        p = Process(target=serve, args=(branch["id"], branch["balance"], branch_ids))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

if __name__ == "__main__":
    start_branches()

