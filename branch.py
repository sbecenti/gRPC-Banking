import grpc
import banks_pb2
import banks_pb2_grpc
import json

class Branch(banks_pb2_grpc.RPCServicer):

    def __init__(self, id, balance, branches):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for debugging purpose
        self.recvMsg = list()

        #initialize the branch logical clock
        self.logical_clock = 0
        # a log of events
        self.event_log = []

        # iterate the processID of the branches
        # store the processID of the branches
        for branch_id in self.branches:
            if branch_id != self.id:

                #identify the port number of the branch based on its processID
                port = 10000 + branch_id
                channel = grpc.insecure_channel(f'localhost:{port}')

                # create a stub for the branch and add it to the stubList
                stub = banks_pb2_grpc.RPCStub(channel)
                self.stubList.append((branch_id, stub))
        
    # process requests from both Client and Branch
    # Args:
    #     request (banks_pb2.Request): The incoming request from the client or another branch.
    #     context (grpc.ServicerContext): The gRPC context.
    # Returns:
    #     banks_pb2.Response: The response to the incoming request.
    def MsgDelivery(self,request, context):

        # increment the branch logical clock only if query or deposit
        if request.interface != "get_events":
            self.update_clock(request.logical_clock)

        # ================================
        # Query logic
        # ================================
        if request.interface == "query":

            # log event
            self.log_event(request.customer_request_id, request.interface, f"event_recv from customer {request.sender_id}")

            return banks_pb2.Response(interface="query", balance=self.balance)

        # Deposit logic
        elif request.interface == "deposit":

            # log event
            self.log_event(request.customer_request_id, request.interface, f"event_recv from customer {request.sender_id}")

            self.balance += request.money

            # create a propogate request to send to the other branches
            for branch_id, stub in self.stubList:

                # increment clock for each propogated branch
                self.increment_clock()

                # log event for the propogated branch
                self.log_event(request.customer_request_id, "propogate_deposit", f"event_sent to branch {branch_id}")

                propogate_request = banks_pb2.Request(
                    interface="propogate_deposit",
                    id=self.id,
                    money=request.money,
                    logical_clock=self.logical_clock,
                    customer_request_id=request.customer_request_id,
                    sender_id=self.id
                )
                stub.MsgDelivery(propogate_request)

            return banks_pb2.Response(interface="deposit", result="success")

        # ================================
        # Withdraw logic
        # ================================
        elif request.interface == "withdraw":

            # log event
            self.log_event(request.customer_request_id, request.interface, f"event_recv from customer {request.sender_id}")

            if self.balance >= request.money:
                self.balance -= request.money

                # create a propogate request to send to the other branches
                for branch_id, stub in self.stubList:

                    # increment clock for each propogated branch
                    self.increment_clock()

                    # log event for the propogated branch
                    self.log_event(request.customer_request_id, "propogate_withdraw", f"event_sent to branch {branch_id}")

                    propogate_request = banks_pb2.Request(
                        interface="propogate_withdraw",
                        id=self.id,
                        money=request.money,
                        logical_clock=self.logical_clock,
                        customer_request_id=request.customer_request_id,
                        sender_id=self.id
                    )
                    stub.MsgDelivery(propogate_request)

                return banks_pb2.Response(interface="withdraw", result="success")

            else:
                return banks_pb2.Response(interface="withdraw", result="fail")

        # ================================
        # propogate_deposit logic
        # ================================
        elif request.interface == "propogate_deposit":

            # log event
            self.log_event(request.customer_request_id, request.interface, f"event_recv from branch {request.sender_id}")        

            self.balance += request.money

            return banks_pb2.Response(interface="propogate_deposit", result="success")

        # ================================
        # propogate_withdraw logic
        # ================================
        elif request.interface == "propogate_withdraw":

            # log event
            self.log_event(request.customer_request_id, request.interface, f"event_recv from branch {request.sender_id}")

            self.balance -= request.money
            return banks_pb2.Response(interface="propogate_withdraw", result="success")

        # ================================
        # get_events logic
        # ================================
        elif request.interface == "get_events":
            return banks_pb2.Response(
                interface="get_events",
                events_json=json.dumps(self.event_log)
            )

    # increment the branch logical clock
    # Args: 
    #     none
    # Returns:
    #     the updated logical clock
    def increment_clock(self):
        self.logical_clock += 1
        return self.logical_clock

    # update the branch logical clock based on the received clock
    # Args:
    #     received_clock
    # Returns:
    #     the updated logical clock
    def update_clock(self, received_clock):
        self.logical_clock = max(self.logical_clock, received_clock) + 1
        return self.logical_clock

    # log branch event
    # Args:
    #     request_id
    #     operation
    #     comment
    # Returns:
    #     none
    def log_event(self, request_id, operation, comment):
        self.event_log.append({
            "customer-request-id": request_id,
            "logical_clock": self.logical_clock,
            "interface": operation,
            "comment": comment
        })


    