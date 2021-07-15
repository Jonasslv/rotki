import json
import logging
import random
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union, overload
from urllib.parse import urlparse
from datetime import datetime

import requests
from ens import ENS
from ens.abis import ENS as ENS_ABI, RESOLVER as ENS_RESOLVER_ABI
from ens.exceptions import InvalidName
from ens.main import ENS_MAINNET_ADDR
from ens.utils import is_none_or_zero_address, normal_name_to_hash, normalize_name
from eth_typing import BlockNumber, HexStr
from typing_extensions import Literal
from web3 import HTTPProvider, Web3
from web3._utils.abi import get_abi_output_types
from web3._utils.contracts import find_matching_event_abi
from web3._utils.filters import construct_event_filter_params
from web3.datastructures import MutableAttributeDict
from web3.exceptions import BadFunctionCallOutput
from web3.middleware.exception_retry_request import http_retry_request_middleware
from web3.types import FilterParams

from rotkehlchen.chain.avalanche.contracts import EthereumContract
from rotkehlchen.chain.avalanche.graph import Graph
from rotkehlchen.chain.avalanche.transactions import EthTransactions
from rotkehlchen.db.dbhandler import DBHandler
from rotkehlchen.errors import (
    BlockchainQueryError,
    DeserializationError,
    InputError,
    RemoteError,
    UnableToDecryptRemoteData,
)
from rotkehlchen.externalapis.covalent import Covalent
from rotkehlchen.fval import FVal
from rotkehlchen.logging import RotkehlchenLogsAdapter
from rotkehlchen.typing import ChecksumEthAddress, SupportedBlockchain, Timestamp
from rotkehlchen.user_messages import MessagesAggregator
from rotkehlchen.utils.misc import from_wei, hex_or_bytes_to_str
from rotkehlchen.utils.network import request_get_dict


logger = logging.getLogger(__name__)
log = RotkehlchenLogsAdapter(logger)

DEFAULT_ETH_RPC_TIMEOUT = 10


def _is_synchronized(current_block: int, latest_block: int) -> Tuple[bool, str]:
    """ Validate that the ethereum node is synchronized
            within 20 blocks of latest block

        Returns a tuple (results, message)
            - result: Boolean for confirmation of synchronized
            - message: A message containing information on what the status is.
    """
    message = ''
    if current_block < (latest_block - 20):
        message = (
            f'Found ethereum node but it is out of sync. {current_block} / '
            f'{latest_block}. Will use etherscan.'
        )
        log.warning(message)
        return False, message

    return True, message


WEB3_LOGQUERY_BLOCK_RANGE = 250000

class AvalancheManager():
    def __init__(
            self,
            avaxrpc_endpoint: str,
            covalent: Covalent,
            database: DBHandler,
            msg_aggregator: MessagesAggregator,
            eth_rpc_timeout: int = DEFAULT_ETH_RPC_TIMEOUT,
    ) -> None:
        log.debug(f'Initializing Ethereum Manager with own rpc endpoint: {avaxrpc_endpoint}')
        self.eth_rpc_timeout = eth_rpc_timeout
        self.w3 = Web3(
            HTTPProvider(
                endpoint_uri=avaxrpc_endpoint,
                request_kwargs={'timeout': self.eth_rpc_timeout},
            )
        )
        self.covalent = covalent
        self.msg_aggregator = msg_aggregator
        #TODO: MUDA PARA O BANCO DA AVAX (quando criar)
        self.transactions = EthTransactions(
            database=database,
            covalent=covalent,
            msg_aggregator=msg_aggregator,
        )
        
    def connected_to_any_web3(self) -> bool:
        return self.w3.isConnected()

    def get_latest_block_number(self) -> int:
            return self.w3.eth.block_number

    def get_avax_balance(self, account: ChecksumEthAddress) -> FVal:
        """Gets the balance of the given account in ETH

        May raise:
        - RemoteError if Etherscan is used and there is a problem querying it or
        parsing its response
        """
        result = self.covalent.get_token_balances_address(account)
        if result is False:
            balance = from_wei(self.w3.eth.get_balance(account))
            return FVal(balance)
        else:
            def filter_avax(value):
                return value['contract_address'] == '0x9debca6ea3af87bf422cea9ac955618ceb56efb4'
            
            avax_coin = list(filter(filter_avax, result))
            balance = from_wei(FVal(avax_coin[0]['balance']))
            return FVal(balance)

    def get_multieth_balance(
            self,
            accounts: List[ChecksumEthAddress],
    ) -> Dict[ChecksumEthAddress, FVal]:
        """Returns a dict with keys being accounts and balances in ETH

        May raise:
        - RemoteError if an external service such as Etherscan is queried and
          there is a problem with its query.
        """
        balances = dict()
        for account in accounts:
            balances[account] = self.get_avax_balance(account)
        return balances

    def get_block_by_number(self, num: int) -> Dict[str, Any]:
        """Returns the block object corresponding to the given block number

        May raise:
        - RemoteError if an external service such as Etherscan is queried and
        there is a problem with its query.
        """

        block_data: MutableAttributeDict = MutableAttributeDict(self.w3.eth.get_block(num))  # type: ignore # pylint: disable=no-member  # noqa: E501
        block_data['hash'] = hex_or_bytes_to_str(block_data['hash'])
        return dict(block_data)

    def get_code(self, account: ChecksumEthAddress) -> str:
        """Gets the deployment bytecode at the given address

        May raise:
        - RemoteError if Etherscan is used and there is a problem querying it or
        parsing its response
        """

        return hex_or_bytes_to_str(self.w3.eth.getCode(account))

    def get_transaction_receipt(
            self,
            tx_hash: str
    ) -> Dict[str, Any]:
        tx_receipt = self.covalent.get_transaction_receipt(tx_hash)
        if tx_receipt is False:
            return self.w3.eth.get_transaction(tx_hash)
        else:
            try:
                # Turn hex numbers to int
                tx_receipt.pop('from_address_label', None)
                tx_receipt.pop('to_address_label', None)
                block_number = tx_receipt['block_height']
                tx_receipt['blockNumber'] = tx_receipt.pop('block_height', None)
                tx_receipt['cumulativeGasUsed'] = tx_receipt.pop('gas_spent', None)
                tx_receipt['gasUsed'] = tx_receipt['cumulativeGasUsed']
                successful = tx_receipt.pop('successful', None)
                tx_receipt['status'] = 1 if successful else 0
                tx_receipt['transactionIndex'] = 0
                tx_receipt['input'] = '0x'
                tx_receipt['nonce'] = 0
                for index, receipt_log in enumerate(tx_receipt['log_events']):
                    receipt_log['blockNumber'] = block_number
                    receipt_log['logIndex'] = receipt_log.pop('log_offset', None)
                    receipt_log['transactionIndex'] = 0
                    tx_receipt['log_events'][index] =  receipt_log
            except (DeserializationError, ValueError) as e:
                raise RemoteError(
                    f'Couldnt deserialize transaction receipt data from covalent {tx_receipt}',
                ) from e
            return tx_receipt

    def call_contract(
            self,
            contract_address: ChecksumEthAddress,
            abi: List,
            method_name: str,
            arguments: Optional[List[Any]] = None,
    ) -> Any:
        """Performs an eth_call to an ethereum contract

        May raise:
        - RemoteError if etherscan is used and there is a problem with
        reaching it or with the returned result
        - BlockchainQueryError if web3 is used and there is a VM execution error
        """

        contract = self.w3.eth.contract(address=contract_address, abi=abi)
        try:
            method = getattr(contract.caller, method_name)
            result = method(*arguments if arguments else [])
        except (ValueError, BadFunctionCallOutput) as e:
            raise BlockchainQueryError(
                f'Error doing call on contract {contract_address}: {str(e)}',
            ) from e
        return result

    def get_logs(self, 
                contract_address: ChecksumEthAddress,
                abi: List,
                event_name: str,
                argument_filters: Dict[str, Any],
                from_timestamp: int,
                to_timestamp: Union[int, Literal['latest']] = 'latest',):
        result =  self.covalent.get_transactions(contract_address, from_timestamp, to_timestamp, True)
        events = list()
        for transaction in result:
            if transaction['log_events']:
                for log in transaction['log_events']:
                    if log['decoded']['name'].lower() == event_name.lower():
                        date = datetime.strptime(log['block_signed_at'], '%Y-%m-%dT%H:%M:%SZ')
                        log['timestamp'] = datetime.timestamp(date)
                        events.append(log)
        return events if len(events) else None

    def get_basic_contract_info(self, address: ChecksumEthAddress) -> Dict[str, Any]:
        """
        Query a contract address and return basic information as:
        - Decimals
        - name
        - symbol
        if it is provided in the contract. This method may raise:
        - BadFunctionCallOutput: If there is an error calling a bad address
        """
        properties = ('decimals', 'symbol', 'name', 'derivedETH')
        info: Dict[str, Any] = {}
        try:
                        # Output contains call status and result
            graph = Graph('https://api.thegraph.com/subgraphs/name/dasconnor/pangolin-dex')
            output = graph.query(
                f'''{{token(id:"{address.lower()}"){{
                    symbol
                    name
                    decimals
                    derivedETH
                    }}
                }}
                '''
            )
            token = output['token']
            for prop in properties:
                info[prop] = token[prop]
        except:
            # If something happens in the connection the output should have
            # the same length as the tuple of properties
            return {'decimals': None, 'symbol': None , 'name': None, 'derivedETH': None}
        return info
