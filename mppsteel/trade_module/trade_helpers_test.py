from mppsteel.trade_module.trade_helpers import return_trade_status, TradeStatus


def test_return_trade_status():
    assert return_trade_status(True, 2) == TradeStatus.EXPORTER
    assert return_trade_status(True, -2) == TradeStatus.DOMESTIC
    assert return_trade_status(False, 2) == TradeStatus.DOMESTIC
    assert return_trade_status(False, -2) == TradeStatus.IMPORTER
