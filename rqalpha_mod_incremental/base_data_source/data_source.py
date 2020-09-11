#!/usr/bin/python3
# -*- coding: utf-8 -*-
# author：Ginta 
# time:2020/9/9
# email: 775650117@qq.com
from datetime import date

from rqalpha.data.base_data_source import BaseDataSource
import datetime


class IncrementcalDataSource(BaseDataSource):
    def __init__(self, path, custom_future_info, start_date):
        super(IncrementcalDataSource, self).__init__(path, custom_future_info)
        self._start_date = start_date

    def available_data_range(self, frequency):
        return self._start_date, datetime.date.max
