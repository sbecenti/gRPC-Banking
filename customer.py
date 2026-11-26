import grpc
import banks_pb2
import banks_pb2_grpc
import time

class Customer:
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = None

        # set of write operation IDs
        self.writeset = set()

        # track last write branch and final balance
        self.last_write_branch = None
        self.final_balance = None

    # create the Customer stub for a specific branch
    def createStub(self, branch_id):
        # create port number based on the branch ID
        port = 10000 + branch_id

        # create a gRPC channel
        channel = grpc.insecure_channel(f'localhost:{port}')

        # create and store a RPCStub
        self.stub = banks_pb2_grpc.RPCStub(channel)


    # send out the events to the Bank
    def executeEvents(self):
        for event in self.events:
            operation = event["interface"]
            event_id = event["id"]

            # determine the branch ID for the event
            branch_id = event["branch"] if "branch" in event else event["dest"]
            self.createStub(branch_id)

            if event["interface"] in ("deposit", "withdraw"):
                self.last_write_branch = branch_id

            # request to send to bank
            if operation == "query":
                request = banks_pb2.Request(interface="query", id=event_id, writeset=list(self.writeset))
                response = self.stub.MsgDelivery(request)

                self.recvMsg.append({
                    "id": event["id"],
                    "interface": "query",
                    "branch": branch_id,
                    "balance": response.balance
                })

            # customer makes a deposit
            elif operation == "deposit":
                money = event["money"]
                request = banks_pb2.Request(interface="deposit", id=event_id, money=money, writeset=list(self.writeset))
                response = self.stub.MsgDelivery(request)

                if response.result == "success":

                    self.writeset.add(response.write_id)   

                    self.recvMsg.append({
                        "id": event["id"],
                        "interface": "deposit",
                        "branch": branch_id,
                        "result": response.result
                    })

            # customer makes a withdrawal
            elif operation == "withdraw":
                money = event["money"]
                request = banks_pb2.Request(interface="withdraw", id=event_id, money=money, writeset=list(self.writeset)) 
                response = self.stub.MsgDelivery(request)

                if response.result == "success":
                    
                    self.writeset.add(response.write_id)

                    self.recvMsg.append({
                        "id": event["id"],
                        "interface": "withdraw",
                        "branch": branch_id,
                        "result": response.result
                    })

            if self.last_write_branch is not None:
                self.createStub(self.last_write_branch)

                request = banks_pb2.Request(interface="query", id=self.id, writeset=list(self.writeset))
                response = self.stub.MsgDelivery(request)
                self.final_balance = response.balance