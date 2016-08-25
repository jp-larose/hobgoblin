# Copyright 2016 ZEROFAIL
#
# This file is part of Goblin.
#
# Goblin is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Goblin is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Goblin.  If not, see <http://www.gnu.org/licenses/>.
import os

import pytest

from goblin import exception


def test_cluster_default_config(cluster):
    assert cluster.config['scheme'] == 'ws'
    assert cluster.config['hosts'] == ['localhost']
    assert cluster.config['port'] == 8182
    assert cluster.config['ssl_certfile'] == ''
    assert cluster.config['ssl_keyfile'] == ''
    assert cluster.config['ssl_password'] == ''
    assert cluster.config['username'] == ''
    assert cluster.config['password'] == ''


@pytest.mark.asyncio
async def test_app_default_config(app):
    assert app.config['scheme'] == 'ws'
    assert app.config['hosts'] == ['localhost']
    assert app.config['port'] == 8182
    assert app.config['ssl_certfile'] == ''
    assert app.config['ssl_keyfile'] == ''
    assert app.config['ssl_password'] == ''
    assert app.config['username'] == ''
    assert app.config['password'] == ''
    await app.close()


def test_cluster_custom_config(event_loop, cluster_class):
    cluster = cluster_class(event_loop, username='dave', password='mypass',
                            hosts=['127.0.0.1'])
    assert cluster.config['scheme'] == 'ws'
    assert cluster.config['hosts'] == ['127.0.0.1']
    assert cluster.config['port'] == 8182
    assert cluster.config['ssl_certfile'] == ''
    assert cluster.config['ssl_keyfile'] == ''
    assert cluster.config['ssl_password'] == ''
    assert cluster.config['username'] == 'dave'
    assert cluster.config['password'] == 'mypass'


def test_cluster_config_from_json(event_loop, cluster_class):
    dirname = os.path.dirname(os.path.dirname(__file__))
    cluster = cluster_class(event_loop)
    cluster.config_from_file(dirname + '/tests/config/config.json')
    assert cluster.config['scheme'] == 'wss'
    assert cluster.config['hosts'] == ['localhost']
    assert cluster.config['port'] == 8182
    assert cluster.config['ssl_certfile'] == ''
    assert cluster.config['ssl_keyfile'] == ''
    assert cluster.config['ssl_password'] == ''
    assert cluster.config['username'] == 'dave'
    assert cluster.config['password'] == 'mypass'