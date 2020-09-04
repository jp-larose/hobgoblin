import pytest
from gremlin_python.process.traversal import Cardinality

from hobgoblin import Hobgoblin, driver, element, properties
from hobgoblin.properties import datatypes
from hobgoblin.provider import TinkerGraph


def pytest_generate_tests(metafunc):
    if 'cluster' in metafunc.fixturenames:
        metafunc.parametrize("cluster", ['c1', 'c2'], indirect=True)


def pytest_addoption(parser):
    parser.addoption(
        '--provider', default='tinkergraph', choices=(
            'tinkergraph',
            'dse',
        ))
    parser.addoption('--gremlin-host', default='localhost')
    parser.addoption('--gremlin-port', default='8182')


def pytest_configure(config):
    config.addinivalue_line("markers", "skip_if_dse: Skip the test if provider is 'dse'")
    config.addinivalue_line("markers", "xfail_if_dse: Fail the test if provider is 'dse'")


def pytest_collection_modifyitems(config, items):
    skip_dse = pytest.mark.skip(reason="Not supported by DSE")
    xfail_dse = pytest.mark.xfail(reason="Fails in DSE")
    if config.getoption('--provider') == 'dse':
        for item in items:
            if 'skip_if_dse' in item.keywords:
                item.add_marker(skip_dse)
            if 'xfail_if_dse' in item.keywords:
                item.add_marker(xfail_dse)


def db_name_factory(x, y):
    return f"{y}__{x}"


class HistoricalName(element.VertexProperty):
    notes = properties.Property(datatypes.String)
    year = properties.Property(datatypes.Integer)  # this is dumb but handy


class Location(element.VertexProperty):
    year = properties.Property(datatypes.Integer)


class Person(element.Vertex):
    _label = 'person'
    name = properties.Property(datatypes.String)
    age = properties.Property(
        datatypes.Integer, db_name='custom__person__age')
    birthplace = element.VertexProperty(datatypes.String)
    location = Location(datatypes.String, card=Cardinality.list_)
    nicknames = element.VertexProperty(
        datatypes.String,
        card=Cardinality.list_,
        db_name_factory=db_name_factory)


class Place(element.Vertex):
    name = properties.Property(datatypes.String)
    zipcode = properties.Property(datatypes.Integer, db_name_factory=db_name_factory)
    historical_name = HistoricalName(datatypes.String, card=Cardinality.list_)
    important_numbers = element.VertexProperty(datatypes.Integer, card=Cardinality.set_)
    incorporated = element.VertexProperty(datatypes.Boolean, default=False)


class Inherited(Person):
    pass


class Knows(element.Edge):
    _label = 'knows'
    notes = properties.Property(datatypes.String, default='N/A')


class LivesIn(element.Edge):
    notes = properties.Property(datatypes.String)


@pytest.fixture
def provider(request):
    provider = request.config.getoption('provider')
    if provider == 'tinkergraph':
        return TinkerGraph
    elif provider == 'dse':
        try:
            import goblin_dse
        except ImportError as e:
            raise RuntimeError(
                "Couldn't run tests with DSEGraph provider: the goblin_dse "
                "package must be installed") from e
        else:
            return goblin_dse.DSEGraph


@pytest.fixture
def aliases(request):
    if request.config.getoption('provider') == 'tinkergraph':
        return {'g': 'g'}
    elif request.config.getoption('provider') == 'dse':
        return {'g': 'testgraph.g'}


@pytest.fixture
def gremlin_server():
    return driver.GremlinServer


@pytest.fixture
def unused_server_url(unused_tcp_port):
    return f'http://localhost:{unused_tcp_port}/gremlin'


@pytest.fixture
def gremlin_host(request):
    return request.config.getoption('gremlin_host')


@pytest.fixture
def gremlin_port(request):
    return request.config.getoption('gremlin_port')


@pytest.fixture
def gremlin_url(gremlin_host, gremlin_port):
    return f"http://{gremlin_host}:{gremlin_port}/gremlin"


@pytest.fixture
async def cluster(request, gremlin_host, gremlin_port, provider, aliases):
    cluster = await driver.Cluster.open(
        hosts=[gremlin_host],
        port=gremlin_port,
        response_timeout=10,
        aliases=aliases,
        provider=provider,
        cluster_name=request.param
    )
    yield cluster

    # Teardown
    if not cluster.closed:
        await cluster.close()


@pytest.fixture
async def connection(gremlin_url, provider):
    try:
        conn = await driver.Connection.open(
                url=gremlin_url,
                message_serializer=driver.GraphSONMessageSerializer,
                response_timeout=10,
                provider=provider)
    except OSError:
        pytest.skip('Gremlin Server is not running')
        return
    yield conn

    # Teardown
    if not conn.closed:
        await conn.close()


@pytest.fixture
async def remote_connection(gremlin_url):
    try:
        remote_conn = await driver.DriverRemoteConnection.open(url=gremlin_url, aliases={'g': 'g'}, response_timeout=10)
    except OSError:
        pytest.skip('Gremlin Server is not running')
        return
    else:
        yield remote_conn

    # Teardown
    if not remote_conn.closed:
        await remote_conn.close()


@pytest.fixture
async def connection_pool(gremlin_url, provider):
    pool = await driver.ConnectionPool.open(
        url=gremlin_url,
        ssl_context=None,
        username='',
        password='',
        max_conns=4,
        min_conns=1,
        max_times_acquired=16,
        max_inflight=64,
        response_timeout=10,
        message_serializer=driver.GraphSONMessageSerializer,
        provider=provider)

    yield pool

    # Teardown
    if not pool.closed:
        await pool.close()


@pytest.fixture
def remote_graph():
    return driver.Graph()


@pytest.fixture
async def app(gremlin_host, gremlin_port, provider, aliases):
    app = await Hobgoblin.open(
            provider=provider,
            aliases=aliases,
            hosts=[gremlin_host],
            port=gremlin_port,
            response_timeout=10
        )

    app.register(Person, Place, Knows, LivesIn)
    yield app

    # Teardown
    if not app.closed:
        await app.close()


# Instance fixtures
@pytest.fixture
def string():
    return datatypes.String()


@pytest.fixture
def integer():
    return datatypes.Integer()


@pytest.fixture
def flt():
    return datatypes.Float()


@pytest.fixture
def boolean():
    return datatypes.Boolean()


@pytest.fixture
def historical_name():
    return HistoricalName()


@pytest.fixture
def person():
    return Person()


@pytest.fixture
def place():
    return Place()


@pytest.fixture
def knows():
    return Knows()


@pytest.fixture
def lives_in():
    return LivesIn()


# @pytest.fixture
# def place_name():
#     return PlaceName()


# Class fixtures
@pytest.fixture
def cluster_class():
    return driver.Cluster


@pytest.fixture
def string_class():
    return datatypes.String


@pytest.fixture
def integer_class():
    return datatypes.Integer


@pytest.fixture
def historical_name_class():
    return HistoricalName


@pytest.fixture
def person_class():
    return Person


@pytest.fixture
def inherited_class():
    return Inherited


@pytest.fixture
def place_class():
    return Place


@pytest.fixture
def knows_class():
    return Knows


@pytest.fixture
def lives_in_class():
    return LivesIn


@pytest.fixture
def flt_class():
    return datatypes.Float


@pytest.fixture
def boolean_class():
    return datatypes.Boolean


@pytest.fixture(autouse=True)
def add_doctest_default(doctest_namespace, tmpdir, event_loop, app):
    doctest_namespace['Person'] = Person
    doctest_namespace['loop'] = event_loop
    doctest_namespace['app'] = app
    config = tmpdir.join('config.yml')
    config.write(
        "scheme: 'ws'\n"
        "hosts: ['localhost']\n"
        "port': 8182\n"
        "ssl_certfile: ''\n"
        "ssl_keyfile: ''\n"
        "ssl_password: ''\n"
        "username: ''\n"
        "password: ''\n"
        "response_timeout: None\n"
        "max_conns: 4\n"
        "min_conns: 1\n"
        "max_times_acquired: 16\n"
        "max_inflight: 64\n"
        "message_serializer: 'hobgoblin.driver.GraphSONMessageSerializer'\n"
    )
    with tmpdir.as_cwd():
        yield
