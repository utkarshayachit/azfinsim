r"""
encapsulates the cache / io operations
"""
import pandas as pd
import redis
import threading
import logging

log = logging.getLogger(__name__)

class TradesCache:
    def __init__(self, mode:str):
        if not mode in ['r', 'w']:
            raise RuntimeError(f'Invalid mode: {mode}')
        self._mode = mode

    def get_trade(self, trade_id:str, column:str='tradenum') -> pd.DataFrame:
        """returns a dataframe with the trade"""
        assert self._mode == 'r'
        raise RuntimeError('Not implemented')

    def set_trades(self, trades: pd.DataFrame, column:str='tradenum') -> None:
        assert self._mode == 'w'
        assert isinstance(trades, pd.DataFrame)
        assert column in trades.columns
        raise RuntimeError('Not implemented')

class TradesCacheRedis(TradesCache):
    """Redis implementation of TradesCache"""
    def __init__(self, redis_client: redis.Redis, mode:str):
        super().__init__(mode)
        self._redis_client = redis_client

    def get_trade(self, trade_id:str, column:str='tradenum') -> pd.DataFrame:
        """returns a dataframe with the trade"""
        assert self._mode == 'r'
        assert isinstance(trade_id, str)
        assert isinstance(column, str)
        data = self._redis_client.get(f'ey{trade_id}.json')
        if data is None:
            raise RuntimeError(f'No trade found for {trade_id}')
        return pd.read_json(data, orient='records')
    
    def set_trades(self, trades: pd.DataFrame, column:str='tradenum') -> None:
        assert self._mode == 'w'
        assert isinstance(trades, pd.DataFrame)
        assert column in trades.columns

        # convert individual trades to json and store in redis
        pipeline = self._redis_client.pipeline()
        for _, row in trades.iterrows():
            trade_id = row[column]
            pipeline.set(f'ey{trade_id}.json', row.to_json(orient='records'))
        pipeline.execute()

class TradesCacheFile(TradesCache):
    """Filesystem implementation of TradesCache"""
    def __init__(self, fname:str, mode:str):
        super().__init__(mode)
        self._fname = fname
        self._lock = threading.Lock()
        self._add_header = True
        self._trades = None

    def get_trade(self, trade_id:str, column:str='tradenum') -> pd.DataFrame:
        """returns a dataframe with the trade"""
        assert self._mode == 'r'
        assert isinstance(trade_id, str)
        assert isinstance(column, str)
        with self._lock:
            if self._trades is None:
                self._trades = pd.read_csv(self._fname, index_col=False)
            return self._trades[self._trades[column] == trade_id]
    
    def set_trades(self, trades: pd.DataFrame, column:str='tradenum') -> None:
        assert self._mode == 'w'
        assert isinstance(trades, pd.DataFrame)
        assert column in trades.columns

        with self._lock:
            # append trades to file
            trades.to_csv(self._fname, mode='w' if self._add_header else 'a', header=self._add_header, index=False)
            self._add_header = False

def connect(args, mode:str) -> TradesCache:
    """connect to the cache"""
    if args.cache_type == 'redis':
        if args.cache_ssl == 'yes':
            return TradesCacheRedis(redis.Redis(\
                host=args.cache_name,
                port=args.cache_port,
                password=args.cache_key,
                ssl_cert_reqs=u'none', #-- or specify location of certs
                ssl=True), mode)
        else:
            return TradesCacheRedis(redis.Redis(\
                host=args.cache_name,
                port=args.cache_port,
                password=args.cache_key), mode)
    elif args.cache_type == 'filesystem':
        return TradesCacheFile(args.cache_path, mode)
    else:
        raise RuntimeError(f'Invalid cache type: {args.cache_type}')
