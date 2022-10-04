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
from rqalpha.const import PERSIST_MODE
from rqalpha_mod_incremental.persist_providers import DiskPersistProvider
from rqalpha.utils.i18n import gettext as _
from rqalpha.data.base_data_source import BaseDataSource

from rqalpha_mod_incremental import persist_providers, recorders
from rqalpha.core.events import EVENT


class IncrementalMod(AbstractMod):
    def __init__(self):
        self._env = None
        self._start_date = None
        self._end_date = None
        self._event_start_time = None
        # 上一次回测时的最后交易日期
        self._last_end_date = None

    def start_up(self, env, mod_config):
        self._env = env
        self._recorder = None
        self._mod_config = mod_config

        if mod_config.recorder == "CsvRecorder":
            if mod_config.persist_folder is None:
                raise RuntimeError(_(u"You need to set persist_folder to use CsvRecorder!"))
        elif mod_config.recorder == "MongodbRecorder":
            if mod_config.strategy_id is None or mod_config.mongo_url is None or mod_config.mongo_dbname is None:
                raise RuntimeError(_(u"MongodbRecorder requires strategy_id, mongo_url and mongo_dbname! "
                                     u"But got {}").format(mod_config))
        else:
            raise RuntimeError(_(u"unknown recorder {}").format(mod_config.recorder))

        config = self._env.config
        if not env.data_source:
            env.set_data_source(BaseDataSource(config.base.data_bundle_path, getattr(config.base, "future_info", {})))

        self._set_env_and_data_source()

        env.config.base.persist = True
        env.config.base.persist_mode = PERSIST_MODE.ON_NORMAL_EXIT

        env.event_bus.add_listener(EVENT.POST_SYSTEM_INIT, self._init)

    def _set_env_and_data_source(self):
        env = self._env
        mod_config = self._mod_config
        system_log.info("use recorder {}", mod_config.recorder)
        if mod_config.recorder == "CsvRecorder":
            persist_folder = os.path.join(mod_config.persist_folder, "persist", str(mod_config.strategy_id))
            persist_provider = DiskPersistProvider(persist_folder)
            self._recorder = recorders.CsvRecorder(persist_folder)
        elif mod_config.recorder == "MongodbRecorder":
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
            event_start_time = datetime.datetime.strptime(persist_meta['last_end_time'],
                                                          '%Y-%m-%d').date() + datetime.timedelta(days=1)
            # 代表历史有运行过，根据历史上次运行的end_date下一天设为事件发送的start_time
            self._meta["origin_start_date"] = persist_meta["origin_start_date"]
            self._meta["start_date"] = persist_meta["start_date"]
            if self._meta["last_end_time"] <= persist_meta["last_end_time"]:
                raise ValueError(
                    'The end_date should after end_date({}) last time'.format(persist_meta["last_end_time"]))
            self._last_end_date = datetime.datetime.strptime(persist_meta["last_end_time"], "%Y-%m-%d").date()
        self._event_start_time = event_start_time
        self._overwrite_event_data_source_func()

    def _overwrite_event_data_source_func(self):
        self._env.data_source.available_data_range = self._available_data_range
        self._env.event_source.events = self._events_decorator(self._env.event_source.events)

    def _available_data_range(self, frequency):
        return self._env.config.base.start_date, datetime.date.max

    def _events_decorator(self, original_events):
        def events(_, __, frequency):
            s, e = self._env.data_source.available_data_range(frequency)
            config_end_date = self._env.config.base.end_date
            event_end_date = e if e < config_end_date else config_end_date
            start_date, end_date = self._event_start_time, event_end_date
            yield from original_events(start_date, end_date, frequency)

        return events

    def _init(self, event):
        env = self._env
        env.event_bus.add_listener(EVENT.TRADE, self.on_trade)
        env.event_bus.prepend_listener(EVENT.POST_SETTLEMENT, self.on_settlement)

    def on_trade(self, event):
        pass

    def on_settlement(self, event):
        """
        当本轮回测当前交易日期大于上一轮回测的最后交易日期时，才允许执行结算事件。
        回测结算是放在before_trading中，在增量回测的过程中会导致出现临界点结算两次。
        """
        if not self._last_end_date or self._env.trading_dt.date() > self._last_end_date:
            return False
        return True

    def tear_down(self, success, exception=None):
        if exception is None:
            self._recorder.store_meta(self._meta)
            self._recorder.flush()
            self._recorder.close()
