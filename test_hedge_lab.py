import tempfile
from hedge_lab import DB, Engine, Market, Book


def test_atomic_pair_completes_and_makes_edge():
    with tempfile.NamedTemporaryFile(suffix='.db') as f:
        cfg = {"starting_capital":10000,"pair_size_usd":100,"max_unhedged_usd":300,
               "max_total_exposure_usd":2000,"min_locked_edge":0.008,"maker_improvement":0.001,
               "hedge_timeout_sec":12,"stale_book_sec":5,"paper_fill_model":"trade_through","db_path":f.name}
        db=DB(f.name,10000); e=Engine(cfg,db)
        m=Market('m','c','s','BTC Up or Down 5m','5m',9999999999,'u','d',0.01)
        e.install_market(m, {'u':Book('u',.45,.45,updated_ms=10**20),'d':Book('d',.45,.45,updated_ms=10**20)})
        e.evaluate_market(m)
        assert e.completed()==1
        assert e.realized > 0
        assert e.exposure()==0
