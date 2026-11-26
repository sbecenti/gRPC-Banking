import grpc
import banks_pb2
import banks_pb2_grpc
import time

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
        # set of write operation IDs
        self.writeset = set()
        # counter to generate write IDs
        self.write_counter = 0

        # iterate the processID of the branches
        # store the processID of the branches
        for branch_id in self.branches:
            if branch_id != self.id:

                #identify the port number of the branch based on its processID
                port = 10000 + branch_id
                channel = grpc.insecure_channel(f'localhost:{port}')

                # create a stub for the branch and add it to the stubList
                stub = banks_pb2_grpc.RPCStub(channel)
                self.stubList.append(stub)
        
    # process requests from both Client and Branch
    # Args:
    #     request (banks_pb2.Request): The incoming request from the client or another branch.
    #     context (grpc.ServicerContext): The gRPC context.
    # Returns:
    #     banks_pb2.Response: The response to the incoming request.
    def MsgDelivery(self,request, context):

        # ensure that all required writes have been applied before processing deposit or withdraw requests
        if request.interface in ("deposit", "withdraw"):
            customer_writeset = set(request.writeset)
            deadline = time.monotonic() + 5

            while not customer_writeset.issubset(self.writeset):
                if time.monotonic() >= deadline:
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "Required writes have not been applied"
                    )
                time.sleep(0.1)

        # Query logic
        if request.interface == "query":
            return banks_pb2.Response(interface="query", balance=self.balance)

        # Deposit logic
        elif request.interface == "deposit":

            # generate a write ID
            write_id = self.generateWriteId()

            # apply deposit and update writeset
            self.balance += request.money
            self.writeset.add(write_id)  

            # create a propogate request to send to the other branches
            for stub in self.stubList:
                propogate_request = banks_pb2.Request(interface="propogate_deposit", id=self.id, money=request.money, write_id=write_id)
                stub.MsgDelivery(propogate_request)

            return banks_pb2.Response(interface="deposit", result="success", write_id=write_id)

        # Withdraw logic
        elif request.interface == "withdraw":
            if self.balance >= request.money:

                # generate write ID
                write_id = self.generateWriteId()

                # apply withdrawal and update writeset
                self.balance -= request.money
                self.writeset.add(write_id)              

                # create a propogate request to send to the other branches
                for stub in self.stubList:
                    propogate_request = banks_pb2.Request(interface="propogate_withdraw", id=self.id, money=request.money, write_id=write_id)
                    stub.MsgDelivery(propogate_request)

                return banks_pb2.Response(interface="withdraw", result="success", write_id=write_id)

            else:
                return banks_pb2.Response(interface="withdraw", result="fail", write_id="")

        # propogate_deposit logic
        elif request.interface == "propogate_deposit":

            # only write to unprocessed write IDs
            if request.write_id not in self.writeset:
                self.balance += request.money
                self.writeset.add(request.write_id)            

            return banks_pb2.Response(interface="propogate_deposit", result="success", write_id=request.write_id)

        # propogate_withdraw logic
        elif request.interface == "propogate_withdraw":
            
            # only write to unprocessed write IDs
            if request.write_id not in self.writeset:
                self.balance -= request.money
                self.writeset.add(request.write_id)         

            return banks_pb2.Response(interface="propogate_withdraw", result="success", write_id=request.write_id)

    # generate ID for a write operation
    # Returns:
    #     str: The generated write operation ID.
    def generateWriteId(self):
        self.write_counter += 1
        return f"{self.id}-{self.write_counter}"