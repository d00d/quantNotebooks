
# coding: utf-8

# # Price_Momentum_Aggregate3
# Ratio of intermediat to short term momentum * LT Momentum
# Take into account aggregate of short term mean reversion and trend.
#     I = 50 D
#     S = 20 D
#     LT = 140
# in the formulation: (I/S)*LT
# 
#  ((close[-1] / close[-50]) / (close[-1] / (close[-20]))* close[-1])

# In[1]:


# Z99-SEPT-2017-Factor_Analysis_Template
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


# In[33]:


# ----- morningstar symbols for factor calculation -------
bs = morningstar.balance_sheet
cfs = morningstar.cash_flow_statement
is_ = morningstar.income_statement
or_ = morningstar.operation_ratios
er = morningstar.earnings_report
v = morningstar.valuation
vr = morningstar.valuation_ratios

# ----- Sector Labels -------
MORNINGSTAR_SECTOR_CODES = {
     -1: 'Misc',
    101: 'Basic Materials',
    102: 'Consumer Cyclical',
    103: 'Financial Services',
    104: 'Real Estate',
    205: 'Consumer Defensive',
    206: 'Healthcare',
    207: 'Utilities',
    308: 'Communication Services',
    309: 'Energy',
    310: 'Industrials',
    311: 'Technology' ,    
}
# --- Factor Algorithm Below -----------------------------
#---------------------------------------------------------
# --- Value Factor ---                    
class Price_Momentum_Aggregate1(CustomFactor):   
   
    #Pre-declare inputs and window_length
    inputs = [USEquityPricing.close] 

    window_length = 140
    
    def compute(self, today, assets, out, close): 
        # Skip the most recent month. 
        # (Ref: Quantitative Momentum: A Practitioner's Guide to Building a Momentum-Based Stock Selection System) 
        # Gray, Wesley R.; Vogel, Jack R.
        
        #out[:] = (close[-1] / close[-20]) - (close[-20] / close[0])
        # I/S*L
        out[:] = ((close[-1] / close[-50]) / (close[-1] / (close[-20]))* close[-1])

#---------------------------------------------------------
# --- Factor Algorithm Above -----------------------------        


# In[34]:


universe = Q1500US()


# In[ ]:


#----- Variables to modify each run -------
#vol = Volatility()
#vol_factor = 'Volatility'

#----- Variables to modify each run -------
factor = Price_Momentum_Aggregate1()
select_factor = 'Price_Momentum_Aggregate1'

# ----- Pipeline construction ----------
pipe = Pipeline(screen=universe)
pipe.add(factor, select_factor) 

# ------- Ranking factor top to bottom -------
factor_rank = factor.rank()

# ------- Ranking factor top to bottom -------
factor_rank = factor.rank()

# ------- Adding Factor Rank to Pipeline as separate column -------
pipe.add(factor_rank, 'factor_rank')

# ------- Adding Sector categorization to Pipeline construction  -------
pipe.add(Sector(), 'Sector')

# ------- Measurement inteval for the analysis -------
#start = pd.Timestamp("2015-01-01")
start = pd.Timestamp("2015-01-01")

#end = pd.Timestamp("2017-06-30")
#start = pd.Timestamp("2017-06-30")
#start = pd.Timestamp("2017-08-01")

end = pd.Timestamp("2017-08-29")

# ------- Run the pipeline and measure runtime -------
start_timer = time()
#results = run_pipeline(pipe, start_date=start, end_date=end).dropna()  
results = run_pipeline(pipe, start_date=start, end_date=end).replace([np.inf, -np.inf], np.nan).dropna()

end_timer = time()


# In[ ]:


print "Time to run pipeline %.2f secs" % (end_timer - start_timer)


# In[ ]:


#results.count() #results.first_valid_index() #results.get_ftype_counts() #results.abs() #results.add_prefix("Z99-")
#results.add_suffix("-Z99") #results.describe() #results.sort #results.tail(50)

# ------- Run the pipeline and measure Runtime to construct pipeline wiht pricing and factor -------
#results.head(100)
#results = np.nan_to_num(results1)
#results.fillna(0).head(100)
#value_table.head(100)
results.head(100)


# In[ ]:


# A MultiIndex DataFrame indexed by date (level 0) and asset (level 1) # [n Date rows x m Equity columns]
assets = results.index.levels[1].unique()

# ------ Get pricing for dates and equities ---------
pricing = get_pricing(assets, start - pd.Timedelta(days=30), end + pd.Timedelta(days=30), fields="close_price")
# FYI... end + pd.Timedelta(days=30) = Timestamp('2017-07-30 00:00:00')
#pricing


# In[ ]:


#al.tears.create_factor_tear_sheet(results[select_factor], pricing, )
#select_factor = 'Price_To_Book2'

# Ingest and format data  
#factor_data = al.utils.get_clean_factor_and_forward_returns(results[select_factor], pricing)

sectors = results['Sector']
factor_data = al.utils.get_clean_factor_and_forward_returns(results[select_factor],
                                                            prices=pricing,
                                                            groupby=sectors,
                                                            groupby_labels=MORNINGSTAR_SECTOR_CODES,
                                                            periods=(1,5,10))


# In[ ]:


# Show factor_data snapshot before running al
factor_data.head(15)


# In[ ]:


# --------- Run AlphaLens Analysis ----------
#al.tears.create_full_tear_sheet(results[select_factor])  

#al.tears.create_full_tear_sheet(factor_data)  
al.tears.create_summary_tear_sheet(factor_data)  


# # AlphaLens Tear Sheet
# Alphalens takes a given factor and examines how useful it is for predicting relative value through a collection of different metrics. The analysis pull various quantifiable signals onto similar scoring scales, then use the factor scores in systematical investing strategies. It breaks all the stocks in a given universe into different quantiles based on their ranking according to your factor and analyzes the returns, information coefficient (IC), the turnover of each quantile, and provides a breakdown of returns and IC by sector.   
# 
# The purpose of our factor analysis is to trace the connection between price performance and underlying stocks’ periodical factor characteristics. Building portfolios of alpha factors allows us to more carefully monitor and analyze the source and consistency of our returns.  Much of this gears to determining if an alpha factor is suitable for a long-short equity algorithm (typically to choose assets to long that will do better than the assets to short) If our ranking scheme is predictive, this means that assets in the top basket will tend to outperform assets in the bottom basket. As long this spread is consistent over time our strategy will have a positive return.  This analysis also provides some feel for correlation & turnover, thus relevant to assess for a long or short strategy respectively.  What we want when comparing factors is to make sure the chosen signal is actually predictive of relative price movements. 
# 
# ### factor_information_coefficient
# Computes the Spearman Rank Correlation based Information Coefficient (IC)between factor values and N period forward returns for each period in the factor index. Params: factor_data : pd.DataFrame - A MultiIndex DataFrame indexed by date (level 0) and asset (level 1), containing the values for a single alpha factor, forward returns for each period, the factor quantile/bin that factor value belongs to, and (optionally) the group the asset belongs to.
# 
# ### factor_rank_autocorrelation
# Autocorrelation of mean factor ranks in specified time spans.  We must compare period to period factor ranks rather than factor values to account for systematic shifts in the factor values of all names or names within a group. This metric is useful for measuring the turnover of a factor. If the value of a factor for each name changes randomly from period to period, we'd expect an autocorrelation of 0.
# 
# ### average_cumulative_return_by_quantile
# Plots sector-wise mean daily returns for factor quantiles across provided forward price movement columns.
# 
# ### quantile_turnover
# Computes the proportion of names in a factor quantile that were not in that quantile in the previous period.
# 
# ### compute_mean_returns_spread
# Computes the difference between the mean returns of two quantiles. Optionally, computes the standard error
# of this difference.
# 
# ### mean_return_by_quantile
# Computes mean returns for factor quantiles across provided forward returns columns.
# 
# ### factor_alpha_beta
# Compute the alpha (excess returns), alpha t-stat (alpha significance), and beta (market exposure) of a factor. A regression is run with the period wise factor universe mean return as the independent variable
# and mean period wise return from a portfolio weighted by factor values as the dependent variable.
# 
# ### factor_returns
#  Computes period wise returns for portfolio weighted by factor values. Weights are computed by demeaning factors and dividing    by the sum of their absolute value (achieving gross leverage of 1).

# Misc Factor Notes: N/A
# 
# 
