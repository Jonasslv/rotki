import logging
from json.decoder import JSONDecodeError
from typing import Any, Dict, List, Optional, Tuple, Union, overload

from datetime import datetime

import gevent
import requests
from typing_extensions import Literal

from rotkehlchen.constants.timing import (
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_READ_TIMEOUT,
    DEFAULT_TIMEOUT_TUPLE,
)
from rotkehlchen.db.dbhandler import DBHandler
from rotkehlchen.errors import ConversionError, DeserializationError, RemoteError
from rotkehlchen.externalapis.interface import ExternalServiceWithApiKey
from rotkehlchen.logging import RotkehlchenLogsAdapter
from rotkehlchen.serialization.deserialize import (
    deserialize_ethereum_address,
    deserialize_int_from_str,
    deserialize_timestamp,
)
from rotkehlchen.typing import ChecksumEthAddress,EthereumTransaction , ExternalService, Timestamp
from rotkehlchen.utils.misc import convert_to_int, hex_or_bytes_to_int, hexstring_to_bytes
from rotkehlchen.user_messages import MessagesAggregator

from web3 import Web3,HTTPProvider

COVALENT_QUERY_LIMIT = 1000

logger = logging.getLogger(__name__)
log = RotkehlchenLogsAdapter(logger)

def read_hash(data: Dict[str, Any], key: str) -> bytes:
    try:
        result = hexstring_to_bytes(data[key])
    except ValueError as e:
        raise DeserializationError(
            f'Failed to read {key} as a hash during etherscan transaction query',
        ) from e
    return result

def read_integer(data: Dict[str, Any], key: str) -> int:
    try:
        result = convert_to_int(data[key])
    except ConversionError as e:
        raise DeserializationError(
            f'Failed to read {key} as an integer during etherscan transaction query',
        ) from e
    return result

def web3_gettransaction(tx_hash):
    w3 = Web3(HTTPProvider("https://api.avax.network/ext/bc/C/rpc"))
    transaction = w3.eth.get_transaction(tx_hash)
    return transaction.input, transaction.nonce

def convert_transaction_from_covalent(
        data: Dict[str, Any],
) -> EthereumTransaction:
    """Reads dict data of a transaction from etherscan and deserializes it

    Can raise DeserializationError if something is wrong
    """
    try:
        # internal tx list contains no gasprice
        timestamp = datetime.timestamp(datetime.strptime(data['block_signed_at'], '%Y-%m-%dT%H:%M:%SZ'))
        input_data, nonce = web3_gettransaction(data['tx_hash'])
        
        return EthereumTransaction(
            timestamp=timestamp,
            block_number=data['block_height'],
            tx_hash=read_hash(data, 'tx_hash'),
            from_address=data['from_address'],
            to_address=data['to_address'],
            value=read_integer(data, 'value'),
            gas=read_integer(data, 'gas_offered'),
            gas_price=read_integer(data, 'gas_price'),
            gas_used=read_integer(data, 'gas_spent'),
            input_data=input_data,
            nonce=nonce,
        )
    except KeyError as e:
        raise DeserializationError(
            f'Etherscan ethereum transaction missing expected key {str(e)}',
        ) from e


class Covalent(ExternalServiceWithApiKey):
    def __init__(self, database: DBHandler, msg_aggregator: MessagesAggregator) -> None:
        super().__init__(database=database, service_name=ExternalService.COVALENT)
        self.msg_aggregator = msg_aggregator
        self.session = requests.session()
        self.warning_given = False
        self.session.headers.update({'User-Agent': 'rotkehlchen'})

    def _query(
            self,
            module: str,
            action: str,
            address: str = None,
            options: Optional[Dict[str, Any]] = None,
            timeout: Optional[Tuple[int, int]] = None,
    ) -> Dict[str, Any]:
        """Queries Covalent

        May raise:
        - RemoteError if there are any problems with reaching Etherscan or if
        an unexpected response is returned
        """
        query_str = f'https://api.covalenthq.com/v1/43114/{action}'
        if address:
            query_str += f'/{address}'
        query_str += f'/{module}/'

        query_str += f'?key=ckey_cb70cd52c833489ea522f104e1a'
        
        if options:
            for name, value in options.items():
                query_str += f'&{name}={value}'

        backoff = 1
        backoff_limit = 33
        while backoff < backoff_limit:
            logger.debug(f'Querying covalent: {query_str}')
            try:
                response = self.session.get(query_str, timeout=timeout if timeout else DEFAULT_TIMEOUT_TUPLE)  # noqa: E501
            except requests.exceptions.RequestException as e:
                if 'Max retries exceeded with url' in str(e):
                    log.debug(
                        f'Got max retries exceeded from Covalent. Will '
                        f'backoff for {backoff} seconds.',
                    )
                    gevent.sleep(backoff)
                    backoff = backoff * 2
                    if backoff >= backoff_limit:
                        raise RemoteError(
                            'Getting Covalent max connections error even '
                            'after we incrementally backed off',
                        ) from e
                    continue

                raise RemoteError(f'Covalent API request failed due to {str(e)}') from e

            if response.status_code != 200:
                raise RemoteError(
                    f'Covalent API request {response.url} failed '
                    f'with HTTP status code {response.status_code} and text '
                    f'{response.text}',
                )

            result = response.json()
            # success, break out of the loop and return result
            return result

        return result

    def get_transactions(
            self,
            account: ChecksumEthAddress,
            from_ts: Optional[Timestamp] = None,
            to_ts: Optional[Timestamp] = None,
    ) -> List[EthereumTransaction]:
        """Gets a list of transactions (either normal or internal) for account.\n
        account is address for wallet.\n
        to_ts is latest date.\n
        from_ts is oldest date.\n
        May raise:
        - RemoteError due to self._query(). Also if the returned result
        is not in the expected format
        """
        if to_ts is None: 
            to_ts = datetime.timestamp(datetime.now())
            
        if from_ts:
            result_master = list()
            for i in range(1,7):
                options = {'limit': COVALENT_QUERY_LIMIT, 'page-size': 8000}
                result = self._query(module='transactions_v2', address=account, action='address', options=options)
                last_date = datetime.strptime(result['data']['items'][-1]['block_signed_at'], '%Y-%m-%dT%H:%M:%SZ')
                result_master += result['data']['items']
                if datetime.timestamp(last_date) <= from_ts:
                    break
                else:
                    options = {'limit': COVALENT_QUERY_LIMIT, 'page-size': 8000, 'skip': i*COVALENT_QUERY_LIMIT}
                    
            def between_date(value):
                date = datetime.strptime(value['block_signed_at'], '%Y-%m-%dT%H:%M:%SZ')
                return datetime.timestamp(date) >= from_ts and datetime.timestamp(date) <= to_ts
        
            list_transactions = list(filter(between_date,result_master))
        else:
            result = self._query(module='transactions_v2', address=account, action='address', options={'limit': 1000, 'page-size': 8000})
            list_transactions = result['data']['items']
            
        transactions = list()
        for transaction in list_transactions:
            transactions.append(convert_transaction_from_covalent(transaction))
        
        return transactions 

    def get_transaction_receipt(self, tx_hash: str) -> Dict[str, Any]:
        """Gets the receipt for the given transaction hash

        May raise:
        - RemoteError due to self._query().
        """
        result = self._query(
            module=tx_hash,
            action='transaction_v2'
        )
        return result
    
    def get_token_balances_address(self, address: ChecksumEthAddress):
        options = {'limit': COVALENT_QUERY_LIMIT, 'page-size': 8000}
        result = self._query(module='balances_v2', address=address, action='address', options=options)
        return result['data']['items']

