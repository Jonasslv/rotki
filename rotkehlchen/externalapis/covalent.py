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
from rotkehlchen.typing import ChecksumEthAddress, EthereumTransaction, ExternalService, Timestamp
from rotkehlchen.user_messages import MessagesAggregator
from rotkehlchen.utils.misc import convert_to_int, hex_or_bytes_to_int, hexstring_to_bytes
from rotkehlchen.utils.serialization import jsonloads_dict

ETHERSCAN_TX_QUERY_LIMIT = 10000

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


class Covalent(ExternalServiceWithApiKey):
    def __init__(self, database: DBHandler, msg_aggregator: MessagesAggregator) -> None:
        super().__init__(database=database, service_name=ExternalService.COVALENT)
        self.msg_aggregator = msg_aggregator
        self.session = requests.session()
        self.warning_given = False
        self.session.headers.update({'User-Agent': 'rotkehlchen'})

    @overload
    def _query(  # pylint: disable=no-self-use
            self,
            module: str,
            action: Literal[
                'balancemulti',
                'txlist',
                'txlistinternal',
                'tokentx',
                'getLogs',
            ],
            options: Optional[Dict[str, Any]] = None,
            timeout: Optional[Tuple[int, int]] = None,
    ) -> List[Dict[str, Any]]:
        ...

    @overload
    def _query(  # pylint: disable=no-self-use
            self,
            module: str,
            action: Literal['eth_getBlockByNumber', 'eth_getTransactionReceipt'],
            options: Optional[Dict[str, Any]] = None,
            timeout: Optional[Tuple[int, int]] = None,
    ) -> Dict[str, Any]:
        ...

    @overload
    def _query(  # pylint: disable=no-self-use
            self,
            module: str,
            action: Literal[
                'balance',
                'tokenbalance',
                'eth_blockNumber',
                'eth_getCode',
                'eth_call',
                'getblocknobytime',
            ],
            options: Optional[Dict[str, Any]] = None,
            timeout: Optional[Tuple[int, int]] = None,
    ) -> str:
        ...

    def _query(
            self,
            module: str,
            action: str,
            address: str = None,
            options: Optional[Dict[str, Any]] = None,
            timeout: Optional[Tuple[int, int]] = None,
    ):
        """Queries etherscan

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
            internal: bool,
            from_ts: Optional[Timestamp] = None,
            to_ts: Optional[Timestamp] = None,
    ) -> List[EthereumTransaction]:
        """Gets a list of transactions (either normal or internal) for account.

        May raise:
        - RemoteError due to self._query(). Also if the returned result
        is not in the expected format
        """
        if from_ts:
            result_master = list()
            for i in range(1,7):
                options = {'limit': 1000, 'page-size': 8000}
                result = self._query(module='transactions_v2', address=account, action='address', options=options)
                timestamp = datetime.strptime(result['data']['items'][-1]['block_signed_at'], '%Y-%m-%dT%H:%M:%SZ')
                result_master += sresult['data']['items']
                if datetime.timestamp(timestamp) <= from_ts:
                    break
                else:
                    options = {'limit': 1000, 'page-size': 8000, 'skip': i*1000}
        
        def between_date(value):
            timestamp = datetime.strptime(value['block_signed_at'], '%Y-%m-%dT%H:%M:%SZ')
            return datetime.timestamp(timestamp) >= from_ts and datetime.timestamp(timestamp) <= to_ts
        
        transactions = list(filter(between_date,result_master))

        return transactions 

    def get_transaction_receipt(self, tx_hash: str) -> Dict[str, Any]:
        """Gets the receipt for the given transaction hash

        May raise:
        - RemoteError due to self._query().
        """
        result = self._query(
            module=tx_hash,
            action='transaction_v2',
        )
        return result

