# -*- coding: utf-8 -*-
#
# Copyright 2017 Ricequant, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""

在策略中  设置config['mod']["incremental"]控制债券mod配置

.. code-block:: python

    __config__ = {
        # 持久化文件夹路径，不设置则不启用mod
        "persist_folder": None,
        # 策略id，持久化的回测将保存在以strategy_id命名的文件夹下
        "strategy_id": 1,
    }
"""

import click
from rqalpha import cli

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


__config__ = {
    # 是否启用 csv 保存 feeds 功能，可以设置为 MongodbRecorder
    "recorder": "CsvRecorder",
    # 当设置为 CsvRecorder 的时候使用，持久化数据输出文件夹
    "persist_folder": None,
    # 当设置为 MongodbRecorder 的时候使用
    "strategy_id": 1,
    "mongo_url": None,
    "mongo_dbname": "rqalpha_records",
    "priority": 111,
}


def load_mod():
    from .mod import IncrementalMod
    return IncrementalMod()


cli_prefix = "mod__incremental__"

cli.commands['run'].params.append(
    click.Option(
        ("--persist-folder", cli_prefix + "persist_folder"),
        help="[incremental] persist folder"
    )
)

cli.commands['run'].params.append(
    click.Option(
        ("--strategy-id", cli_prefix + "strategy_id"),
        help="[incremental] strategy id "
    )
)

cli.commands['run'].params.append(
    click.Option(
        ("--recorder", cli_prefix + "recorder"),
        type=click.Choice(["CsvRecorder", "MongodbRecorder"]),
        help="[incremental] recorder name"
    )
)

cli.commands['run'].params.append(
    click.Option(
        ("--mongo-url", cli_prefix + "mongo_url"),
        help="[incremental] recorder mongo url"
    )
)

