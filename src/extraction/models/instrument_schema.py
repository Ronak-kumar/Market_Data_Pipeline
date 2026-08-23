import polars as pl


MASTER_INSTRUMENT_SCHEMA = {
    "instrument_key": pl.String,
    "exchange_token": pl.String,

    "exchange": pl.String,
    "segment": pl.String,
    "instrument_type": pl.String,
    "security_type": pl.String,

    "trading_symbol": pl.String,
    "name": pl.String,
    "short_name": pl.String,
    "isin": pl.String,

    "asset_key": pl.String,
    "asset_symbol": pl.String,
    "asset_type": pl.String,

    "underlying_key": pl.String,
    "underlying_symbol": pl.String,
    "underlying_type": pl.String,

    "expiry": pl.Date,
    "strike_price": pl.Float64,
    "option_type": pl.String,
    "weekly": pl.Boolean,

    "lot_size": pl.Int64,
    "minimum_lot": pl.Int64,
    "freeze_quantity": pl.Int64,
    "tick_size": pl.Float64,
    "qty_multiplier": pl.Float64,

    "last_trading_date": pl.Date,

    "start_time": pl.Time,
    "end_time": pl.Time,
    "week_days": pl.String,
}