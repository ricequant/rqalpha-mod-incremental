import os
import datetime

from rqalpha.interface import AbstractPersistProvider


class MongodbPersistProvider(AbstractPersistProvider):
    def __init__(self, strategy_id, mongo_url, mongo_db):
        import pymongo
        import gridfs

        persist_db = pymongo.MongoClient(mongo_url)[mongo_db]
        self._strategy_id = strategy_id
        self._fs = gridfs.GridFS(persist_db)

    def store(self, key, value):
        update_time = datetime.datetime.now()
        self._fs.put(value, strategy_id=self._strategy_id, key=key, update_time=update_time)
        for grid_out in self._fs.find(
                {"strategy_id": self._strategy_id, "key": key, "update_time": {"$lt": update_time}}):
            self._fs.delete(grid_out._id)

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
if __name__ == '__main__':
    mg = MongodbPersistProvider(1,2,3)