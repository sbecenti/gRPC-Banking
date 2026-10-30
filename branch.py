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
        for branch_id in self.branches:
            if branch_id != self.id:

                #identify the port number of the branch based on its processID
                port = 5000 + branch_id
                channel = grpc.insecure_channel(f'localhost:{port}')

                # create a stub for the branch and add it to the stubList
                stub = banks_pb2_grpc.RPCStub(channel)
                self.stubList.append(stub)

        print("Number of stubs created: ", len(self.stubList))

        # TODO: students are expected to store the processID of the branches
        pass

    # TODO: students are expected to process requests from both Client and Branch
    def MsgDelivery(self,request, context):

        # Query logic
        if request.interface == "query":
            return banks_pb2.Response(interface="query", balance=self.balance)

        # Deposit logic
        elif request.interface == "deposit":
            self.balance += request.money
            return banks_pb2.Response(interface="deposit", result="success")

        # Withdraw logic
        elif request.interface == "withdraw":
            if self.balance >= request.money:
                self.balance -= request.money
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


if __name__ == "__main__":
    branch = Branch(id=2, balance=1000, branches=[1, 2, 3])
            