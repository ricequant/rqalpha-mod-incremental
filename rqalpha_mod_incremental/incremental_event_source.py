#!/usr/bin/python3
# -*- coding: utf-8 -*-
# author：Ginta 
# time:2020/9/8
# email: 775650117@qq.com

from rqalpha.mod.rqalpha_mod_sys_simulation.simulation_event_source import SimulationEventSource


class IncrementalEventSource(SimulationEventSource):
    def __init__(self, env, event_start_date, event_end_date):
        super(IncrementalEventSource, self).__init__(env)
        self._event_start_date = event_start_date
        self._event_end_date = event_end_date

    def events(self, start_date, end_date, frequency):
        return super(IncrementalEventSource, self).events(self._event_start_date, self._event_end_date, frequency)
