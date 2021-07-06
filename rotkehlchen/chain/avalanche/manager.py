import json
import logging
import random
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union, overload
from urllib.parse import urlparse

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
from rotkehlchen.chain.avalanche.node_avalanche_web3 import Node_Avalanche_Web3
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


def _query_web3_get_logs(
        web3: Web3,
        filter_args: FilterParams,
        from_block: int,
        to_block: Union[int, Literal['latest']],
        contract_address: ChecksumEthAddress,
        event_name: str,
        argument_filters: Dict[str, Any],
) -> List[Dict[str, Any]]:
    until_block = web3.eth.block_number if to_block == 'latest' else to_block
    events: List[Dict[str, Any]] = []
    start_block = from_block

    block_range = initial_block_range = WEB3_LOGQUERY_BLOCK_RANGE


    while start_block <= until_block:
        filter_args['fromBlock'] = start_block
        end_block = min(start_block + block_range, until_block)
        filter_args['toBlock'] = end_block
        log.debug(
            'Querying web3 node for contract event',
            contract_address=contract_address,
            event_name=event_name,
            argument_filters=argument_filters,
            from_block=filter_args['fromBlock'],
            to_block=filter_args['toBlock'],
        )
        # As seen in https://github.com/rotki/rotki/issues/1787, the json RPC, if it
        # is infura can throw an error here which we can only parse by catching the  exception
        try:
            new_events_web3: List[Dict[str, Any]] = [dict(x) for x in web3.eth.get_logs(filter_args)]  # noqa: E501
        except ValueError as e:
            try:
                decoded_error = json.loads(str(e).replace("'", '"'))
            except json.JSONDecodeError:
                # reraise the value error if the error is not json
                raise e from None

            msg = decoded_error.get('message', '')
            # errors from: https://infura.io/docs/ethereum/json-rpc/eth-getLogs
            if msg in ('query returned more than 10000 results', 'query timeout exceeded'):
                block_range = block_range // 2
                if block_range < 50:
                    raise  # stop retrying if block range gets too small
                # repeat the query with smaller block range
                continue
            # else, well we tried .. reraise the Value error
            raise e

        # Turn all HexBytes into hex strings
        for e_idx, event in enumerate(new_events_web3):
            new_events_web3[e_idx]['blockHash'] = event['blockHash'].hex()
            new_topics = []
            for topic in event['topics']:
                new_topics.append(topic.hex())
            new_events_web3[e_idx]['topics'] = new_topics
            new_events_web3[e_idx]['transactionHash'] = event['transactionHash'].hex()

        start_block = end_block + 1
        events.extend(new_events_web3)
        # end of the loop, end of 1 query. Reset the block range to max
        block_range = initial_block_range

    return events

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
        self.node_web3: Node_Avalanche_Web3
        self.own_rpc_endpoint = avaxrpc_endpoint
        self.covalent = covalent
        self.msg_aggregator = msg_aggregator
        self.eth_rpc_timeout = eth_rpc_timeout
        self.transactions = EthTransactions(
            database=database,
            covalent=covalent,
            msg_aggregator=msg_aggregator,
        )
        
    def connected_to_any_web3(self) -> bool:
        return self.node_web3.w3.isConnected()

    def query(self, method: Callable, **kwargs: Any) -> Any:
        """Queries ethereum related data by performing the provided method to all given nodes

        The first node in the call order that gets a succcesful response returns.
        If none get a result then a remote error is raised
        """
        try:
            result = method(self.node_web3.w3, **kwargs)
        except (RemoteError, BlockchainQueryError, requests.exceptions.RequestException) as e:
            log.warning(f'Failed to query Avalanche RPC for {str(method)} due to {str(e)}')
            # Catch all possible errors here and just try next node call
        else:
            return result
        # no node in the call order list was succesfully queried
        raise RemoteError(
            f'Failed to query {str(method)} after trying the following '
        )

    def _get_latest_block_number(self, web3: Optional[Web3]) -> int:
        if web3 is not None:
            return web3.eth.block_number

    def get_latest_block_number(self) -> int:
        return self.query(
            method=self._get_latest_block_number
        )

    def get_eth_balance(self, account: ChecksumEthAddress) -> FVal:
        """Gets the balance of the given account in ETH

        May raise:
        - RemoteError if Etherscan is used and there is a problem querying it or
        parsing its response
        """
        result = self.get_multieth_balance([account])
        return result[account]

    def get_multieth_balance(
            self,
            accounts: List[ChecksumEthAddress],
    ) -> Dict[ChecksumEthAddress, FVal]:
        """Returns a dict with keys being accounts and balances in ETH

        May raise:
        - RemoteError if an external service such as Etherscan is queried and
          there is a problem with its query.
        """

        return [False]

    def get_block_by_number(
            self,
            num: int,
    ) -> Dict[str, Any]:
        return self.query(
            method=self._get_block_by_number,
            num=num,
        )

    def _get_block_by_number(self, web3: Optional[Web3], num: int) -> Dict[str, Any]:
        """Returns the block object corresponding to the given block number

        May raise:
        - RemoteError if an external service such as Etherscan is queried and
        there is a problem with its query.
        """

        block_data: MutableAttributeDict = MutableAttributeDict(web3.eth.get_block(num))  # type: ignore # pylint: disable=no-member  # noqa: E501
        block_data['hash'] = hex_or_bytes_to_str(block_data['hash'])
        return dict(block_data)

    def get_code(
            self,
            account: ChecksumEthAddress,
    ) -> str:
        return self.query(
            method=self._get_code,
            account=account,
        )

    def _get_code(self, web3: Optional[Web3], account: ChecksumEthAddress) -> str:
        """Gets the deployment bytecode at the given address

        May raise:
        - RemoteError if Etherscan is used and there is a problem querying it or
        parsing its response
        """

        return hex_or_bytes_to_str(web3.eth.getCode(account))

    def _get_transaction_receipt(
            self,
            web3: Optional[Web3],
            tx_hash: str,
    ) -> Dict[str, Any]:
        if web3 is None:
            tx_receipt = self.covalent.get_transaction_receipt(tx_hash)
            try:
                # Turn hex numbers to int
                block_number = tx_receipt['blockNumber']
                tx_receipt['blockNumber'] = block_number
                tx_receipt['cumulativeGasUsed'] = tx_receipt['gas_spent']
                tx_receipt['gasUsed'] = tx_receipt['gas_spent']
                tx_receipt['status'] = 1 if tx_receipt['successful'] else 0
                tx_receipt['transactionIndex'] = 0
                for receipt_log in tx_receipt['log_events']:
                    receipt_log['blockNumber'] = block_number
                    receipt_log['logIndex'] = receipt_log['log_offset']
                    receipt_log['transactionIndex'] = 0
            except (DeserializationError, ValueError) as e:
                raise RemoteError(
                    f'Couldnt deserialize transaction receipt data from covalent {tx_receipt}',
                ) from e
            return tx_receipt

        tx_receipt = web3.eth.get_transaction_receipt(tx_hash)  # type: ignore
        return tx_receipt

    def get_transaction_receipt(
            self,
            tx_hash: str
    ) -> Dict[str, Any]:
        return self.query(
            method=self._get_transaction_receipt,
            tx_hash=tx_hash,
        )

    def call_contract(
            self,
            contract_address: ChecksumEthAddress,
            abi: List,
            method_name: str,
            arguments: Optional[List[Any]] = None,
    ) -> Any:
        return self.query(
            method=self._call_contract,
            contract_address=contract_address,
            abi=abi,
            method_name=method_name,
            arguments=arguments,
        )

    def _call_contract(
            self,
            web3: Optional[Web3],
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

        contract = self.node_web3.w3.eth.contract(address=contract_address, abi=abi)
        try:
            method = getattr(contract.caller, method_name)
            result = method(*arguments if arguments else [])
        except (ValueError, BadFunctionCallOutput) as e:
            raise BlockchainQueryError(
                f'Error doing call on contract {contract_address}: {str(e)}',
            ) from e
        return result

    def get_logs(
            self,
            contract_address: ChecksumEthAddress,
            abi: List,
            event_name: str,
            argument_filters: Dict[str, Any],
            from_block: int,
            to_block: Union[int, Literal['latest']] = 'latest',
    ) -> List[Dict[str, Any]]:
        return self.query(
            method=self._get_logs,
            contract_address=contract_address,
            abi=abi,
            event_name=event_name,
            argument_filters=argument_filters,
            from_block=from_block,
            to_block=to_block,
        )

    def _get_logs(
            self,
            web3: Optional[Web3],
            contract_address: ChecksumEthAddress,
            abi: List,
            event_name: str,
            argument_filters: Dict[str, Any],
            from_block: int,
            to_block: Union[int, Literal['latest']] = 'latest',
    ) -> List[Dict[str, Any]]:
        """Queries logs of an ethereum contract

        May raise:
        - RemoteError if etherscan is used and there is a problem with
        reaching it or with the returned result
        """
        event_abi = find_matching_event_abi(abi=abi, event_name=event_name)
        _, filter_args = construct_event_filter_params(
            event_abi=event_abi,
            abi_codec=Web3().codec,
            contract_address=contract_address,
            argument_filters=argument_filters,
            fromBlock=from_block,
            toBlock=to_block,
        )
        if event_abi['anonymous']:
            # web3.py does not handle the anonymous events correctly and adds the first topic
            filter_args['topics'] = filter_args['topics'][1:]
        events: List[Dict[str, Any]] = []
        if web3 is not None:
            events = _query_web3_get_logs(
                web3=web3,
                filter_args=filter_args,
                from_block=from_block,
                to_block=to_block,
                contract_address=contract_address,
                event_name=event_name,
                argument_filters=argument_filters,
            )

        return events

    def get_event_timestamp(self, event: Dict[str, Any]) -> Timestamp:
        """Reads an event returned either by etherscan or web3 and gets its timestamp

        Etherscan events contain a timestamp. Normal web3 events don't so it needs to
        be queried from the block number

        WE could also add this to the get_logs() call but would add unnecessary
        rpc calls for get_block_by_number() for each log entry. Better have it
        lazy queried like this.

        TODO: Perhaps better approach would be a log event class for this
        """
        if 'timeStamp' in event:
            # event from etherscan
            return Timestamp(event['timeStamp'])

        # event from web3
        block_number = event['blockNumber']
        block_data = self.get_block_by_number(block_number)
        return Timestamp(block_data['timestamp'])

    def get_basic_contract_info(self, address: ChecksumEthAddress) -> Dict[str, Any]:
        """
        Query a contract address and return basic information as:
        - Decimals
        - name
        - symbol
        if it is provided in the contract. This method may raise:
        - BadFunctionCallOutput: If there is an error calling a bad address
        """
        properties = ('decimals', 'symbol', 'name')
        info: Dict[str, Any] = {}

        try:
                        # Output contains call status and result
            graph = Graph('https://api.thegraph.com/subgraphs/name/dasconnor/pangolin-dex')
            output = graph.query(
                f'''{{token(id:"{address}"){{
                    symbol
                    name
                    decimals
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
            return {'decimals': None, 'symbol': None , 'name': None}
        return info
