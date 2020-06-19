Using Graph (GLV)
=================

:py:mod:`~hobgoblin.app.Hobgoblin` provides access to the underlying :py:mod:`aiogremlin`
asynchronous version of the Gremlin-Python Gremlin Language Variant (GLV) that
is bundled with Apache TinkerPop beginning with the 3.2.2 release. Traversals are
generated using the class
:py:class:`Graph<aiogremlin.structure.graph.Graph>` combined with a remote
connection class, either
:py:class:`DriverRemoteConnection<aiogremlin.remote.driver_remote_connection.DriverRemoteConnection>`::

    >>> import asyncio
    >>> import hobgoblin  # provides aliases to common aiogremlin objects

    >>> loop = asyncio.get_event_loop()
    >>> remote_conn = loop.run_until_complete(
    ...     hobgoblin.DriverRemoteConnection.open(
    ...         "http://localhost:8182/gremlin", 'g'))
    >>> graph = hobgoblin.driver.Graph()
    >>> g = graph.traversal().withRemote(remote_conn)

Once you have a traversal source, it's all Gremlin...::

    >>> traversal = g.addV('query_language').property('name', 'gremlin')

`traversal` is in an instance of
:py:class:`AsyncGraphTraversal<aiogremlin.process.graph_traversal.AsyncGraphTraversal>`, which
implements the Python 3.5 asynchronous iterator protocol::

    >>> async def iterate_traversal(traversal):
    ...     async for msg in traversal:
    ...         print(msg)

    >>> loop.run_until_complete(iterate_traversal(traversal))
    v[...]

:py:class:`AsyncGraphTraversal<aiogremlin.process.graph_traversal.AsyncGraphTraversal>` also
provides several convenience coroutine methods to help iterate over results:

- :py:meth:`next<aiogremlin.process.graph_traversal.AsyncGraphTraversal.next>`
- :py:meth:`toList<aiogremlin.process.graph_traversal.AsyncGraphTraversal.toList>`
- :py:meth:`toSet<aiogremlin.process.graph_traversal.AsyncGraphTraversal.toSet>`

Notice the mixedCase? Not very pythonic? Well no, but it maintains continuity
with the Gremlin query language, and that's what the GLV is all about...

Note: Gremlin steps that are reserved words in Python, like `or`, `in`, use a
a trailing underscore `or_` and `in_`.

The Side Effect Interface
-------------------------

When using TinkerPop 3.2.2+ with the default
:py:mod:`Hobgoblin<hobgoblin.app.Hobgoblin>` provides an asynchronous side effects interface using the
:py:class:`AsyncRemoteTraversalSideEffects<aiogremlin.remote.driver_remote_side_effects.AsyncRemoteTraversalSideEffects>`
class. This allows side effects to be retrieved after executing the traversal::

    >>> traversal = g.V().aggregate('a')
    >>> loop.run_until_complete(traversal.iterate())
    [['V'], ['aggregate', 'a']]

Calling
:py:meth:`keys<aiogremlin.remote.driver_remote_side_effects.AsyncRemoteTraversalSideEffects.keys>`
will return an asynchronous iterator containing all keys for cached
side effects:

    >>> async def get_side_effect_keys(traversal):
    ...     keys = await traversal.side_effects.keys()
    ...     print(keys)

    >>> loop.run_until_complete(get_side_effect_keys(traversal))
    {'a'}

Then calling
:py:meth:`get<aiogremlin.remote.driver_remote_side_effects.AsyncRemoteTraversalSideEffects.get>`
using a valid key will return the cached side effects::

    >>> async def get_side_effects(traversal):
    ...     se = await traversal.side_effects.get('a')
    ...     print(se)


    >>> loop.run_until_complete(get_side_effects(traversal))
    {v[1]: 1, ...}

And that's it! For more information on Gremlin Language Variants, please
visit the `Apache TinkerPop GLV Documentation`_.


.. _Apache TinkerPop GLV Documentation: http://tinkerpop.apache.org/docs/3.2.2/tutorials/gremlin-language-variants/
