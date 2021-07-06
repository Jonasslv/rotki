from web3 import Web3, HTTPProvider

class Node_Avalanche_Web3():
    def __init__(self,rpc_endpoint):
        self.rpc_endpoint = rpc_endpoint
        w3 = Web3(HTTPProvider(self.rpc_endpoint))
        