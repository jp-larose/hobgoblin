Configuring the :py:mod:`Hobgoblin<hobgoblin>` App Object
===================================================

The :py:class:`Hobgoblin<hobgoblin.app.Hobgoblin>` object generally supports the same
configuration options as
:py:class:`Cluster<aiogremlin.driver.cluster.Cluster>`. Please see the
:doc:`driver docs</driver>` for a complete list of configuration parameters.


The :py:class:`Hobgoblin<hobgoblin.app.Hobgoblin>` object should be created using the
:py:class:`open<hobgoblin.app.Hobgoblin.open>` classmethod, and configuration can
be passed as keyword arguments, or loaded from a config file::

    >>> import asyncio
    >>> from hobgoblin import Hobgoblin

    >>> loop = asyncio.get_event_loop()

    >>> app = loop.run_until_complete(Hobgoblin.open(loop))
    >>> app.config_from_file('config.yml')

Contents of `config.yml`::

    scheme: 'ws'
    hosts: ['localhost']
    port': 8182
    ssl_certfile: ''
    ssl_keyfile: ''
    ssl_password: ''
    username: ''
    password: ''
    response_timeout: null
    max_conns: 4
    min_conns: 1
    max_times_acquired: 16
    max_inflight: 64
    message_serializer: 'hobgoblin.driver.GraphSONMessageSerializer'

Special :py:mod:`Hobgoblin<hobgoblin>` App Configuration
--------------------------------------------------------------

:py:class:`Hobgoblin<hobgoblin.app.Hobgoblin>` supports two additional configuration
keyword parameters: `aliases` and `get_hashable_id`.

`aliases`
~~~~~~~~~

`aliases` as stated in the TinkerPop docs: are "a map of key/value pairs that
allow globally bound Graph and TraversalSource objects to be aliased to
different variable names for purposes of the current request". Setting the
aliases on the :py:class:`Hobgoblin<hobgoblin.app.Hobgoblin>` object provides a default
for this value to be passed on each request.

`get_hashable_id`
~~~~~~~~~~~~~~~~~

`get_hashable_id` is a callable that translates a graph id into a hash
that can be used to map graph elements in the
:py:class:`Session<hobgoblin.session.Session>` element cache. In many cases,
it is not necessary to provide a value for this keyword argument. For example,
TinkerGraph assigns integer IDs that work perfectly for this purpose. However,
other provider implementations, such as DSE, use more complex data structures
to represent element IDs. In this case, the application developer must provide a
hashing function. For example, the following recipe takes an id map and uses
its values to produces a hashable id::

    >>> def get_id_hash(dict):
    ...     hashes = map(hash, dict.items())
    ...     id_hash = functools.reduce(operator.xor, hashes, 0)
    ...     return id_hash

Look for provider specific :py:mod:`Hobgoblin<hobgoblin>` libraries in the near
future!
