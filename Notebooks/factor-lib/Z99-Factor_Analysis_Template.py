from quantopian.research import run_pipeline
from quantopian.pipeline import Pipeline
from quantopian.pipeline.factors import Latest
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.factors import CustomFactor, SimpleMovingAverage, AverageDollarVolume, Returns, RSI
from quantopian.pipeline.classifiers.morningstar import Sector
from quantopian.pipeline.filters import Q500US, Q1500US
from time import time

import talib
import pandas as pd
import numpy as np

import alphalens as al
import pyfolio as pf
from scipy import stats
import matplotlib.pyplot as plt

# ----- morningstar symbols for factor calculation -------
bs = morningstar.balance_sheet
cfs = morningstar.cash_flow_statement
is_ = morningstar.income_statement
or_ = morningstar.operation_ratios
er = morningstar.earnings_report
v = morningstar.valuation
vr = morningstar.valuation_ratios

# --- Factor Algorithm Below -----------------------------
#---------------------------------------------------------
class Price_To_Book(CustomFactor):   
   
    # Pre-declare inputs and window_length
    inputs = [USEquityPricing.close, vr.book_value_per_share] 
    window_length = 1
     
        # Compute market cap value
    def compute(self, today, assets, out, close, bv):       
        out[:] = close[-1] / bv[-1]  

#---------------------------------------------------------
# --- Factor Algorithm Above -----------------------------        

class Volatility (CustomFactor):
    
    inputs = [USEquityPricing.close]
    window_length = 252
    
    def compute(self, today, assets, out, close):  
        close = pd.DataFrame(data=close, columns=assets) 
        # Going to rank largest we'll need to invert the sdev.
        # Annualized volatility is the standard deviation of logarithmic returns 
        # as a Ratio of its mean divided by 1/ sqrt of the number of trading days
        out[:] = 1 / np.log(close).diff().std()
        
        universe = Q1500US()

        #----- Variables to modify each run -------
vol = Volatility()
vol_factor = 'Volatility'

#----- Variables to modify each run -------
factor = Price_To_Book()
select_factor = 'Price_To_Book'

# ----- Pipeline construction ----------
pipe = Pipeline(screen=universe)
pipe.add(factor, select_factor) 

# ------- Ranking factor top to bottom -------
factor_rank = factor.rank()

# ------- Ranking factor top to bottom -------
factor_rank = factor.rank()

# ------- Adding Factor Rank to Pipeline as separate column -------
pipe.add(factor_rank, 'factor_rank')

# ------- Measurement inteval for the analysis -------
#start = pd.Timestamp("2015-01-01")
start = pd.Timestamp("2015-01-01")

#end = pd.Timestamp("2017-06-30")
#start = pd.Timestamp("2017-06-30")
#start = pd.Timestamp("2017-08-01")

end = pd.Timestamp("2017-08-29")

# ------- Run the pipeline and measure runtime -------
start_timer = time()
results = run_pipeline(pipe, start_date=start, end_date=end)                                
end_timer = time()
#results.fillna(value=0);

#results.count() #results.first_valid_index() #results.get_ftype_counts() #results.abs() #results.add_prefix("Z99-")
#results.add_suffix("-Z99") #results.describe() #results.sort #results.tail(50)

# ------- Run the pipeline and measure Runtime to construct pipeline wiht pricing and factor -------
results.head(100)

# A MultiIndex DataFrame indexed by date (level 0) and asset (level 1) # [n Date rows x m Equity columns]
assets = results.index.levels[1].unique()

# ------ Get pricing for dates and equities ---------
pricing = get_pricing(assets, start - pd.Timedelta(days=30), end + pd.Timedelta(days=30), fields="close_price")
# FYI... end + pd.Timedelta(days=30) = Timestamp('2017-07-30 00:00:00')
#pricing

#al.tears.create_factor_tear_sheet(results[select_factor], pricing, )
#select_factor = 'Price_To_Book2'

# Ingest and format data  
factor_data = al.utils.get_clean_factor_and_forward_returns(results[select_factor], pricing)

# Show factor_data snapshot before running al
factor_data.head(15)

# Run AlphaLens Analysis  
al.tears.create_full_tear_sheet(factor_data)  







