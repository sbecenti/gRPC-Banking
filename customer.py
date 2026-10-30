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

    # create the Customer stub
    def createStub(self):
        # create port number based on the customer ID
        port = 10000 + self.id

        # create a gRPC channel
        channel = grpc.insecure_channel(f'localhost:{port}')

        # create and store a RPCStub
        self.stub = banks_pb2_grpc.RPCStub(channel)


    # send out the events to the Bank
    def executeEvents(self):
        for event in self.events:
            operation = event["interface"]
            event_id = event["id"]

            # request to send to bank
            if operation == "query":
                request = banks_pb2.Request(interface="query", id=event_id)
                response = self.stub.MsgDelivery(request)

                self.recvMsg.append({
                    "interface": "query",
                    "balance": response.balance
                })

            # customer makes a deposit
            elif operation == "deposit":
                money = event["money"]
                request = banks_pb2.Request(interface="deposit", id=event_id, money=money)
                response = self.stub.MsgDelivery(request)

                if response.result == "success":
                    self.recvMsg.append({
                        "interface": "deposit",
                        "result": response.result
                    })

            # customer makes a withdrawal
            elif operation == "withdraw":
                money = event["money"]
                request = banks_pb2.Request(interface="withdraw", id=event_id, money=money)
                response = self.stub.MsgDelivery(request)

                if response.result == "success":
                    self.recvMsg.append({
                        "interface": "withdraw",
                        "result": response.result
                    })