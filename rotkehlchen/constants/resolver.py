from rotkehlchen.typing import ChecksumEthAddress

ETHEREUM_DIRECTIVE = '_ceth_'
ETHEREUM_DIRECTIVE_LENGTH = len(ETHEREUM_DIRECTIVE)

def ethaddress_to_identifier(address: ChecksumEthAddress) -> str:
    return ETHEREUM_DIRECTIVE + address


def strethaddress_to_identifier(address: str) -> str:
    return ETHEREUM_DIRECTIVE + address

AVALANCHE_DIRECTIVE = '_avax_'
AVALANCHE_DIRECTIVE_LENGTH = len(ETHEREUM_DIRECTIVE)

def avaxaddress_to_identifier(address: ChecksumEthAddress) -> str:
    return AVALANCHE_DIRECTIVE + address


def stravaxaddress_to_identifier(address: str) -> str:
    return AVALANCHE_DIRECTIVE + address
