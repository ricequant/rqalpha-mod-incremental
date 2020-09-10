#!/usr/bin/python3
# -*- coding: utf-8 -*-
# author：Ginta 
# time:2020/9/8
# email: 775650117@qq.com
import datetime

from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.mod.rqalpha_mod_sys_simulation.simulation_event_source import SimulationEventSource
from rqalpha.utils.datetime_func import convert_int_to_date

class IncrementalEventSource(SimulationEventSource):
    def __init__(self, env, event_start_date):
        super(IncrementalEventSource, self).__init__(env)
        self._event_start_date = event_start_date

    def events(self, start_date, end_date, frequency):
        s, e = self._env.data_source._day_bars[INSTRUMENT_TYPE.INDX].get_date_range('000001.XSHG')
        return super(IncrementalEventSource, self).events(self._event_start_date, convert_int_to_date(e), frequency)
