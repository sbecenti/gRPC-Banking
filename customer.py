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

        #initialize the customer logical clock
        self.logical_clock = 0
        # a log of events
        self.event_log = []

    # create the Customer stub
    def createStub(self):
        # create port number based on the customer ID
        port = 10000 + self.id

        # create a gRPC channel
        channel = grpc.insecure_channel(f'localhost:{port}')

        # create and store a RPCStub
        self.stub = banks_pb2_grpc.RPCStub(channel)


    # send out the events to the Bank
    # Args:
    #     None
    # Returns:
    #     None
    def executeEvents(self):
        for event in self.events:
            operation = event["interface"]
            event_id = event["id"]

            # increment the customer logical clock
            self.increment_clock()

            # log the customer event
            self.log_event(event_id, operation)

            # ================================
            # request to send to bank
            # ================================
            if operation == "query":
                request = banks_pb2.Request(
                    interface="query", 
                    id=event_id,
                    logical_clock=self.logical_clock,
                    customer_request_id=event_id,
                    sender_id=self.id
                )
                response = self.stub.MsgDelivery(request)

                self.recvMsg.append({
                    "interface": "query",
                    "balance": response.balance
                })

            # ================================
            # customer makes a deposit
            # ================================
            elif operation == "deposit":
                money = event["money"]
                request = banks_pb2.Request(
                    interface="deposit", 
                    id=event_id, money=money, 
                    logical_clock=self.logical_clock, 
                    customer_request_id=event_id, 
                    sender_id=self.id
                )
                response = self.stub.MsgDelivery(request)

                if response.result == "success":
                    self.recvMsg.append({
                        "interface": "deposit",
                        "result": response.result
                    })

            # ================================
            # customer makes a withdrawal
            # ================================
            elif operation == "withdraw":
                money = event["money"]
                request = banks_pb2.Request(
                    interface="withdraw", 
                    id=event_id, money=money, 
                    logical_clock=self.logical_clock, 
                    customer_request_id=event_id, 
                    sender_id=self.id
                )
                response = self.stub.MsgDelivery(request)

                if response.result == "success":
                    self.recvMsg.append({
                        "interface": "withdraw",
                        "result": response.result
                    })

    # increment the customer logical clock
    # Args:
    #     None
    # Returns:
    #     int: The updated logical clock.
    def increment_clock(self):
        self.logical_clock += 1
        return self.logical_clock

    # update the customer logical clock based on the received clock
    # Args:
    #     received_clock (int): The received logical clock.
    # Returns:
    #     int: The updated logical clock.
    def update_clock(self, received_clock):
        self.logical_clock = max(self.logical_clock, received_clock) + 1
        return self.logical_clock

    # log customer event
    # Args:
    #     request_id (int): The customer request ID.
    #     operation (str): The operation performed.
    # Returns:
    #     None
    def log_event(self, request_id, operation):
        self.event_log.append({
            "customer-request-id": request_id,
            "logical_clock": self.logical_clock,
            "interface": operation,
            "comment": f"event_sent from customer {self.id}"
        })
