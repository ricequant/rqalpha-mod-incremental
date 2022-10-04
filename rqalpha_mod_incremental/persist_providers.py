import os
import datetime
import jsonpickle
import pandas as pd
from rqrisk import Risk
from rqalpha.interface import AbstractPersistProvider


def get_performance(strategy_id, analysis_data):
    daily_returns = analysis_data['portfolio_daily_returns']
    benchmark = analysis_data['benchmark_daily_returns']
    dates = [p['date'] for p in analysis_data['total_portfolios']]
    assert len(daily_returns) == len(benchmark) == len(dates), 'unmatched length'
    daily_returns = pd.Series(daily_returns, index=dates)
    benchmark = pd.Series(benchmark, index=dates)
    risk = Risk(daily_returns, benchmark, 0.)
    perf = risk.all()
    perf['strategy_id'] = strategy_id
    perf['update_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    perf['start_date'] = analysis_data['total_portfolios'][0]['date'].strftime('%Y-%m-%d')
    perf['end_date'] = analysis_data['total_portfolios'][-1]['date'].strftime('%Y-%m-%d')
    return perf


class MongodbPersistProvider(AbstractPersistProvider):
    def __init__(self, strategy_id, mongo_url, mongo_db):
        import pymongo
        import gridfs

        self.persist_db = pymongo.MongoClient(mongo_url)[mongo_db]
        self._strategy_id = strategy_id
        self._fs = gridfs.GridFS(self.persist_db)

    def store(self, key, value):
        update_time = datetime.datetime.now()
        self._fs.put(value, strategy_id=self._strategy_id, key=key, update_time=update_time)
        for grid_out in self._fs.find(
                {"strategy_id": self._strategy_id, "key": key, "update_time": {"$lt": update_time}}):
            self._fs.delete(grid_out._id)
        if key == "mod_sys_analyser":
            self._store_performance(value)

    def _store_performance(self, analysis_data):
        try:
            perf = get_performance(self._strategy_id,
                                   jsonpickle.loads(analysis_data.decode("utf-8")))
            self.persist_db['performance'].update({"strategy_id": self._strategy_id}, perf, upsert=True)
        except Exception as e:
            print(e)

    def load(self, key, large_file=False):
        import gridfs
        try:
            b = self._fs.get_last_version(strategy_id=self._strategy_id, key=key)
            return b.read()
        except gridfs.errors.NoFile:
            return None

    def should_resume(self):
        return False

    def should_run_init(self):
        return False


class DiskPersistProvider(AbstractPersistProvider):
    def __init__(self, path="./persist"):
        self._path = path
        try:
            os.makedirs(self._path)
        except:
            pass

    def store(self, key, value):
        assert isinstance(value, bytes), "value must be bytes"
        with open(os.path.join(self._path, key), "wb") as f:
            f.write(value)

    def load(self, key, large_file=False):
        try:
            with open(os.path.join(self._path, key), "rb") as f:
                return f.read()
        except IOError as e:
            return None

    def should_resume(self):
        return False

    def should_run_init(self):
        return False
