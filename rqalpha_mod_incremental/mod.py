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
from rqalpha.environment import Environment
from rqalpha_mod_incremental.persist_providers import DiskPersistProvider
from rqalpha.utils.i18n import gettext as _
from rqalpha.data.base_data_source import BaseDataSource
from rqalpha_mod_incremental import persist_providers, recorders
from rqalpha.core.events import EVENT


class IncrementalMod(AbstractMod):
    def __init__(self):
        self._event_start_time = None
        self._last_end_date = None  # 上一次回测时的最后交易日期
        self._recorder = None

    def start_up(self, env: Environment, mod_config):
        if not getattr(env, "data_source", None):
            env.set_data_source(BaseDataSource(env.config.base))
        self._set_env_and_data_source(env, mod_config)

        env.config.base.persist = True
        env.config.base.persist_mode = PERSIST_MODE.ON_NORMAL_EXIT

        env.event_bus.add_listener(EVENT.POST_SYSTEM_INIT, self._init)

    def _set_env_and_data_source(self, env: Environment, mod_config):
        self._env = env
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
            "origin_start_date": env.config.base.start_date.strftime("%Y-%m-%d"),
            "start_date": env.config.base.start_date.strftime("%Y-%m-%d"),
            "end_date": env.config.base.end_date.strftime("%Y-%m-%d"),
            "last_end_time": env.config.base.end_date.strftime("%Y-%m-%d"),
            "last_run_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        event_start_time = env.config.base.start_date
        persist_meta = self._recorder.load_meta()
        if persist_meta:
            # 不修改回测开始时间
            env.config.base.start_date = datetime.datetime.strptime(persist_meta['start_date'], '%Y-%m-%d').date()
            event_start_time = datetime.datetime.strptime(persist_meta['last_end_time'], '%Y-%m-%d').date() + datetime.timedelta(days=1)
            # 代表历史有运行过，根据历史上次运行的end_date下一天设为事件发送的start_time
            self._meta["origin_start_date"] = persist_meta["origin_start_date"]
            self._meta["start_date"] = persist_meta["start_date"]
            if self._meta["last_end_time"] <= persist_meta["last_end_time"]:
                raise ValueError('The end_date should after end_date({}) last time'.format(persist_meta["last_end_time"]))
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
        self._env.event_bus.add_listener(EVENT.TRADE, self.on_trade)
        self._env.event_bus.prepend_listener(EVENT.POST_SETTLEMENT, self.on_settlement)
        self._env.event_bus.add_listener(EVENT.BEFORE_SYSTEM_RESTORED, self.on_before_system_restored)

    def on_before_system_restored(self, event):
        # 此时end_date已经经过调整，重新保存
        self._meta["last_end_time"] = self._env.config.base.end_date.strftime("%Y-%m-%d")
        if self._last_end_date:
            # 将日期调整为上一个区间的结束日期的收盘时间段
            _datetime = datetime.datetime.combine(self._last_end_date, datetime.time(hour=15, minute=30, second=0))
            self._env.update_time(_datetime, _datetime)

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

    def tear_down(self, code, exception=None):
        if self._recorder is None:
            return
        if exception is None:
            self._recorder.store_meta(self._meta)
            self._recorder.flush()
            self._recorder.close()
