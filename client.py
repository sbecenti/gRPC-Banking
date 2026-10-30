import grpc
import banks_pb2
import banks_pb2_grpc

def main():

    port = 5001
    channel = grpc.insecure_channel(f'localhost:{port}')
    stub = banks_pb2_grpc.RPCStub(channel)

    request = banks_pb2.Request(interface="query", id=1)
    response = stub.MsgDelivery(request)
    print(f"Branch {1} has balance: {response.balance}")

    request = banks_pb2.Request(interface="deposit", id=1, money=100)
    response = stub.MsgDelivery(request)
    print(f"Branch {1} deposited $100, result: {response.result}")

    request = banks_pb2.Request(interface="withdraw", id=2, money=50)
    response = stub.MsgDelivery(request)
    print(f"Branch {2} withdrew $50, result: {response.result}")

    for branch_id in [1, 25, 40, 50]:

        port = 5000 + branch_id
        channel = grpc.insecure_channel(f'localhost:{port}')
        stub = banks_pb2_grpc.RPCStub(channel)

        request = banks_pb2.Request(interface="query", id=branch_id)
        response = stub.MsgDelivery(request)
        print(f"Branch {branch_id} has balance: {response.balance}")

if __name__ == "__main__":
    main()