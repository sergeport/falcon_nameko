# falcon_nameko

Middleware for using nameko services with Falcon

> NOTE: it is still in development, there are debugprints and other rubbish.
> Will be completed soon

# Usage

```python
import falcon
from falcon_nameko import NamekoClusterProxyMiddleware as Rpc


application: falcon.API = falcon.API(middleware=[
    Rpc({
        'NAMEKO_AMQP_URI': config.AMQP_URI,
        'NAMEKO_INITIAL_CONNECTIONS': 2,
        'NAMEKO_MAX_CONNECTIONS': 8,
        'NAMEKO_EXCLUSIVE_REQUEST_CONNECTION': False
    })
])

application.add_route('/api', RpcRouter())

# decorator for exclusove connection:
# @Rpc.exclusive_connection(True)


```


More coming soon...

