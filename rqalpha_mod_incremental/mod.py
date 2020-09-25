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

import os
import datetime

from rqalpha.utils.logger import system_log
from rqalpha.interface import AbstractMod
from rqalpha.core.events import EVENT
from rqalpha.const import PERSIST_MODE
from rqalpha_mod_incremental.persist_providers import DiskPersistProvider
from rqalpha.utils.i18n import gettext as _

from rqalpha_mod_incremental import persist_providers, recorders
from rqalpha_mod_incremental.incremental_event_source import IncrementalEventSource
from rqalpha_mod_incremental.base_data_source.data_source import IncrementcalDataSource
from rqalpha.const import INSTRUMENT_TYPE, DEFAULT_ACCOUNT_TYPE, MARKET, TRADING_CALENDAR_TYPE
from rqalpha.utils.datetime_func import convert_int_to_date
from rqalpha.core.events import Event, EVENT


class IncrementalMod(AbstractMod):
    def __init__(self):
        self._env = None
        self._start_date = None
        self._end_date = None
        self._event_start_time = None

    def start_up(self, env, mod_config):
        self._env = env
        self._recorder = None
        self._mod_config = mod_config

        if not self._mod_config.persist_folder:
            return

        self._set_env_and_data_source()

        env.config.base.persist = True
        env.config.base.persist_mode = PERSIST_MODE.ON_NORMAL_EXIT

        env.event_bus.add_listener(EVENT.POST_SYSTEM_INIT, self._init)

    def _set_env_and_data_source(self):
        env = self._env
        mod_config = self._mod_config
        system_log.info("use recorder {}", mod_config.recorder)
        if mod_config.recorder == "CsvRecorder":
            if not mod_config.persist_folder:
                raise RuntimeError(_(u"You need to set persist_folder to use CsvRecorder"))
            persist_folder = os.path.join(mod_config.persist_folder, "persist", str(mod_config.strategy_id))
            persist_provider = DiskPersistProvider(persist_folder)
            self._recorder = recorders.CsvRecorder(persist_folder)
        elif mod_config.recorder == "MongodbRecorder":
            if mod_config.strategy_id is None:
                raise RuntimeError(_(u"You need to set strategy_id"))
            persist_provider = persist_providers.MongodbPersistProvider(mod_config.strategy_id, mod_config.mongo_url,
                                                                        mod_config.mongo_dbname)
            self._recorder = recorders.MongodbRecorder(mod_config.strategy_id,
                                                       mod_config.mongo_url, mod_config.mongo_dbname)
        else:
            raise RuntimeError(_(u"unknown recorder {}").format(mod_config.recorder))

        if env.persist_provider is None:
            env.set_persist_provider(persist_provider)

        self._meta = {
            "strategy_id": mod_config.strategy_id,
            "origin_start_date": self._env.config.base.start_date.strftime("%Y-%m-%d"),
            "start_date": self._env.config.base.start_date.strftime("%Y-%m-%d"),
            "end_date": self._env.config.base.end_date.strftime("%Y-%m-%d"),
            "last_end_time": self._env.config.base.end_date.strftime("%Y-%m-%d"),
            "last_run_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        event_start_time = self._env.config.base.start_date
        persist_meta = self._recorder.load_meta()
        if persist_meta:
            # 不修改回测开始时间
            self._env.config.base.start_date = datetime.datetime.strptime(persist_meta['start_date'], '%Y-%m-%d').date()
            event_start_time = datetime.datetime.strptime(persist_meta['last_end_time'], '%Y-%m-%d').date() + datetime.timedelta(days=1)
            # 代表历史有运行过，根据历史上次运行的end_date下一天设为事件发送的start_time
            self._meta["origin_start_date"] = persist_meta["origin_start_date"]
            self._meta["start_date"] = persist_meta["start_date"]
            if self._meta["last_end_time"] <= persist_meta["last_end_time"]:
                raise ValueError('The end_date should after end_date({}) last time'.format(persist_meta["last_end_time"]))
        self._event_start_time = event_start_time
        self._overwrite_event_data_source_func()

    def _overwrite_event_data_source_func(self):
        self._env.data_source.available_data_range = self._available_data_range
        self._env.event_source.events = self._events

    def _available_data_range(self, frequency):
        return self._env.config.base.start_date, datetime.date.max

    def _events(self, start_date, end_date, frequency):
        s, e = self._env.data_source._day_bars[INSTRUMENT_TYPE.INDX].get_date_range('000001.XSHG')
        config_end_date = self._env.config.base.end_date
        event_end_date = convert_int_to_date(e) if convert_int_to_date(e).date() < config_end_date else config_end_date
        start_date, end_date = self._event_start_time, event_end_date
        self._env.event_source.events(start_date, end_date, frequency)


    def _init(self, event):
        env = self._env
        env.event_bus.add_listener(EVENT.TRADE, self.on_trade)
        env.event_bus.add_listener(EVENT.POST_SETTLEMENT, self.on_settlement)

    def on_trade(self, event):
        pass

    def on_settlement(self, event):
        pass

    def tear_down(self, success, exception=None):
        if not self._mod_config.persist_folder:
            return
        if exception is None:
            self._recorder.store_meta(self._meta)
            self._recorder.flush()
            self._recorder.close()

