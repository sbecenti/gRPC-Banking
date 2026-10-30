import grpc
import banks_pb2
import banks_pb2_grpc

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
    def MsgDelivery(self,request, context):

        # Query logic
        if request.interface == "query":
            return banks_pb2.Response(interface="query", balance=self.balance)

        # Deposit logic
        elif request.interface == "deposit":
            self.balance += request.money

            # create a propogate request to send to the other branches
            for stub in self.stubList:
                propogate_request = banks_pb2.Request(interface="propogate_deposit", id=self.id, money=request.money)
                stub.MsgDelivery(propogate_request)

            return banks_pb2.Response(interface="deposit", result="success")

        # Withdraw logic
        elif request.interface == "withdraw":
            if self.balance >= request.money:
                self.balance -= request.money

                # create a propogate request to send to the other branches
                for stub in self.stubList:
                    propogate_request = banks_pb2.Request(interface="propogate_withdraw", id=self.id, money=request.money)
                    stub.MsgDelivery(propogate_request)

                return banks_pb2.Response(interface="withdraw", result="success")

            else:
                return banks_pb2.Response(interface="withdraw", result="fail")

        # propogate_deposit logic
        elif request.interface == "propogate_deposit":
            self.balance += request.money
            return banks_pb2.Response(interface="propogate_deposit", result="success")

        # propogate_withdraw logic
        elif request.interface == "propogate_withdraw":
            self.balance -= request.money
            return banks_pb2.Response(interface="propogate_withdraw", result="success")