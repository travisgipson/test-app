import datetime as dt
import yfinance as yf
import pandas_market_calendars as mcal

# local  modules
from . import taskparams
from . import data
from . import creds

def run_main_task():

    # set query parameters
    tickers = taskparams.tickers
    current_date = str(dt.date.today())
    current_time = dt.datetime.now().time()

    # set market calendar
    nyse_cal  = mcal.get_calendar("NYSE")
    valid_mkt_days = [str(t.date()) for t in nyse_cal.valid_days(current_date,current_date)]

    # run only when market has been open
    if current_date in valid_mkt_days:

        #  ensure we're past market close (5pm central)
        if current_time >= dt.time(hour=1):

            # create postgres engine
            psql = data.create_psql_eng(creds.psql)

            # download today's data
            ohlc = yf.download(tickers=tickers,period="1d") 

            # stack data and reformat headers
            df = ohlc.stack().reset_index().rename({"level_1":"ticker"},axis=1)
            df.columns= [c.replace(" ","_").lower() for c in df.columns]

            # save data to postgres
            df.to_sql(name="etfs",schema="prices",if_exists="append",con=psql,index=False)
            psql.close()