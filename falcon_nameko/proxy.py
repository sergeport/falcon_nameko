from nameko.standalone.rpc import ClusterRpcProxy
from typing import Dict, Any
from .errors import BadConfigurationError, ClusterNotConfiguredError
from .pool import ConnectionPool
from .connection import Connection


class NamekoClusterRpcProxy:
    _pool = None
    config = {}
    _context = None
    _exclusive_request_connection = False

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config
        self.configure()

    @property
    def release_connection_after_call(self):
        if self.context and hasattr(self.context, 'exclusive_request_rpc_connection'):
            return not self.context.exclusive_request_rpc_connection
        return not self._exclusive_request_connection

    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, context):
        self._context = context

    def configure(self):
        if not self.config.get('AMQP_URI'):
            raise BadConfigurationError("AMQP connection URI missing.")
        self._pool = ConnectionPool(
            connection_producer=self._nameko_connection_producer,
            min_connections=self.config.get('INITIAL_CONNECTIONS', 2),
            max_connections=self.config.get('MAX_CONNECTIONS', 8),
            recycle_frequency=self.config.get('POOL_RECYCLE', 0)
        )
        self._exclusive_request_connection = self.config.get('EXCLUSIVE_REQUEST_CONNECTION',
                                                             self._exclusive_request_connection)

    def _nameko_connection_producer(self):
        proxy = ClusterRpcProxy(
            self.config,
            timeout=self.config.get('RPC_TIMEOUT', None)
        )
        return proxy.start()

    def get_connection(self) -> Connection:
        if not (hasattr(self.context, 'nameko_connection') and self.context.nameko_connection):
            if not self._pool:
                raise ClusterNotConfiguredError("Cluster is not configured.")
            self.context.nameko_connection = self._pool.next()
        return self.context.nameko_connection

    def release_context_connection(self):
        if hasattr(self.context, 'nameko_connection') and self.context.nameko_connection is not None:
            self._pool.release(self.context.nameko_connection)
            self.context.nameko_connection = None

    def call_service(self, name):
        result = getattr(self.get_connection(), name)
        if self.release_connection_after_call:
            print(f'releasing connection after calling service "{name}"')
            self.release_context_connection()
        return result
