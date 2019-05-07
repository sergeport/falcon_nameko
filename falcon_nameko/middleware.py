import re
from typing import Dict, Any, Callable
from six import iteritems
import falcon
from .errors import BadConfigurationError, ClusterNotConfiguredError
from .proxy import NamekoClusterRpcProxy


class NamekoClusterProxyMiddleware:
    """
    FlaskPooledClusterRpcProxy accepts all nameko configuration values, prefixed with the NAMEKO_ prefix. In addition, it exposes additional configuration options:

    NAMEKO_INITIAL_CONNECTIONS (int, default=2) - the number of initial connections to the Nameko cluster to create
    NAMEKO_MAX_CONNECTIONS (int, default=8) - the max number of connections to the Nameko cluster to create before raises an error
    NAMEKO_EXCLUSIVE_REQUEST_CONNECTION (bool, default=False) - whether connections to services should be released after the service is accessed (False) or after request is processed (True). True can be used when you need to hold the connection during lifecycle of request (if you need to make few service calls)
    NAMEKO_RPC_TIMEOUT (int, default=None) - the default timeout before raising an error when trying to call a service RPC method
    NAMEKO_POOL_RECYCLE (int, default=None) - if specified, connections that are older than this interval, specified in seconds, will be automatically recycled on checkout. This setting is useful for environments where connections are happening through a proxy like HAProxy which automatically closes connections after a specified interval.
    """
    _proxy: NamekoClusterRpcProxy = None

    def __init__(self, config: Dict[str, Any] = None):
        self._proxy = NamekoClusterRpcProxy(self.filter_config(config))

    def filter_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        result = {}
        if not config:
            raise BadConfigurationError("No configuration provided.")
        for k, v in iteritems(config):
            match = re.match(r"NAMEKO\_(?P<name>.*)", k)
            if match:
                result[match.group('name')] = v
        return result

    @staticmethod
    def exclusive_connection(value: bool = True):
        def decorator(func):
            def wrapper(self, req, resp, *args, **kwargs):
                self.exclusive_request_rpc_connection = value
                return func(self, req, resp, *args, **kwargs)

            return wrapper

        return decorator

    def process_resource(self, req: falcon.Request, resp: falcon.Response, resource: object, params: Any):
        resource.rpc = RpcService(service_caller=self._proxy.call_service)
        self._proxy.context = resource

    def process_response(self, req: falcon.Request, resp: falcon.Response, resource: object, req_succeeded: bool):
        self._proxy.release_context_connection()


class RpcService:

    def __init__(self, service_caller: Callable):
        self.service_caller = service_caller

    def __getattr__(self, name):
        return self.service_caller(name)
