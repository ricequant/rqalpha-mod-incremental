#!/usr/bin/python3
# -*- coding: utf-8 -*-
# author：Ginta 
# time:2020/9/8
# email: 775650117@qq.com
from itertools import chain
from datetime import timedelta, datetime, time

import pandas

from rqalpha.environment import Environment
from rqalpha.interface import AbstractEventSource
from rqalpha.events import Event, EVENT
from rqalpha.utils.exception import patch_user_exc
from rqalpha.const import DEFAULT_ACCOUNT_TYPE, MARKET, TRADING_CALENDAR_TYPE
from rqalpha.utils.i18n import gettext as _


class IncrementalEventSource(AbstractEventSource):
    def __init__(self, env, event_start_date, event_end_date):
        self._env = env
        self._event_start_date = event_start_date
        self._event_end_date = event_end_date
        self._config = env.config
        self._universe_changed = False
        self._env.event_bus.add_listener(EVENT.POST_UNIVERSE_CHANGED, self._on_universe_changed)

        if self._config.base.market == MARKET.CN:
            self._get_day_bar_dt = lambda date: date.replace(hour=15, minute=0)
            self._get_after_trading_dt = lambda date: date.replace(hour=15, minute=30)
        elif self._config.base.market == MARKET.HK:
            self._get_day_bar_dt = lambda date: date.replace(hour=16, minute=6)
            self._get_after_trading_dt = lambda date: date.replace(hour=16, minute=30)
        else:
            raise NotImplementedError(_("Unsupported market {}".format(self._config.base.market)))

    def _on_universe_changed(self, _):
        self._universe_changed = True

    def _get_universe(self):
        universe = self._env.get_universe()
        if len(universe) == 0 and DEFAULT_ACCOUNT_TYPE.STOCK.name not in self._config.base.accounts:
            raise patch_user_exc(RuntimeError(_(
                "Current universe is empty. Please use subscribe function before trade"
            )), force=True)
        return universe

    def _get_merged_trading_dates(self, start_date, end_date, trading_calendar_types):
        if len(trading_calendar_types) == 1:
            return self._env.data_proxy.get_trading_dates(start_date, end_date, trading_calendar_types[0])
        trading_calendars = []
        for calendar_type in trading_calendar_types:
            trading_calendars.append(self._env.data_proxy.get_trading_dates(start_date, end_date, calendar_type))
        return pandas.DatetimeIndex(chain(*trading_calendars)).unique()

    def events(self, start_date, end_date, frequency):
        calendar_types = []
        for account_type in self._config.base.accounts:
            if account_type in (DEFAULT_ACCOUNT_TYPE.STOCK, DEFAULT_ACCOUNT_TYPE.FUTURE):
                calendar_types.append(TRADING_CALENDAR_TYPE.EXCHANGE)
            elif account_type == DEFAULT_ACCOUNT_TYPE.BOND:
                calendar_types.append(TRADING_CALENDAR_TYPE.INTER_BANK)
        trading_dates = self._get_merged_trading_dates(self._event_start_date, self._event_end_date, calendar_types)

        if frequency == "1d":
            # 根据起始日期和结束日期，获取所有的交易日，然后再循环获取每一个交易日
            for day in trading_dates:
                date = day.to_pydatetime()
                dt_before_trading = date.replace(hour=0, minute=0)

                dt_bar = self._get_day_bar_dt(date)
                dt_after_trading = self._get_after_trading_dt(date)

                yield Event(EVENT.BEFORE_TRADING, calendar_dt=dt_before_trading, trading_dt=dt_before_trading)
                yield Event(EVENT.OPEN_AUCTION, calendar_dt=dt_before_trading, trading_dt=dt_before_trading)
                yield Event(EVENT.BAR, calendar_dt=dt_bar, trading_dt=dt_bar)
                yield Event(EVENT.AFTER_TRADING, calendar_dt=dt_after_trading, trading_dt=dt_after_trading)
        else:
            raise NotImplementedError(_("Frequency {} is not support.").format(frequency))
