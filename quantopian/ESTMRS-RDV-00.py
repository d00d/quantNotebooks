"""

This algorithm enhances a simple five-day mean reversion strategy by:
1. Skipping the last day's return
2. Sorting stocks based on the volatility of the five day return, to get steady moves vs jumpy ones
I also commented out two other filters that I looked at: 
1. Six month volatility
2. Liquidity (volume/(shares outstanding))

"""

import numpy as np
import pandas as pd
from quantopian.pipeline import Pipeline
from quantopian.pipeline import CustomFactor
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import SimpleMovingAverage, AverageDollarVolume
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.filters import Q500US

# The job of initialize() is to perform any one-time startup logic.  called exactly once when our algorithm starts and requires context as input.
# Context, an augmented Python dictionary, is used for maintaining state during backtest or live trading, and can be referenced in different 
# parts of algorithm. Should be used instead of global variables in the algorithm. Properties can be accessed using dot notation (context.some_property).
def initialize(context):
    
    # Set benchmark to short-term Treasury note ETF (SHY) since strategy is dollar neutral
    set_benchmark(sid(23911))
    
    # Schedule our rebalance function to run at the end of each day.
    schedule_function(my_rebalance, date_rules.every_day(), time_rules.market_close())

    # Record variables at the end of each day.
    schedule_function(my_record_vars, date_rules.every_day(), time_rules.market_close())
    
    # Get intraday prices today before the close if you are not skipping the most recent data
    schedule_function(get_prices,date_rules.every_day(), time_rules.market_close(minutes=5))
    
    # Set commissions and slippage to 0 to determine pure alpha
    set_commission(commission.PerShare(cost=0, min_trade_cost=0))
    set_slippage(slippage.FixedSlippage(spread=0))
    
    # Number of quantiles for sorting returns for mean reversion
    context.nq=5
    
    # Number of quantiles for sorting volatility over five-day mean reversion period
    context.nq_vol=3
    

    # Create our pipeline and attach it to our algorithm.
    my_pipe = make_pipeline()
    attach_pipeline(my_pipe, 'my_pipeline')

class Volatility(CustomFactor):  
    inputs = [USEquityPricing.close]
    window_length=132
    
    def compute(self, today, assets, out, close):
        # I compute 6-month volatility, starting before the five-day mean reversion period
        daily_returns = np.log(close[1:-6]) - np.log(close[0:-7])
        out[:] = daily_returns.std(axis = 0)           

class Liquidity(CustomFactor):   
    inputs = [USEquityPricing.volume, morningstar.valuation.shares_outstanding] 
    window_length = 1
    
    def compute(self, today, assets, out, volume, shares):       
        out[:] = volume[-1]/shares[-1]        
        
class Sector(CustomFactor):
    inputs=[morningstar.asset_classification.morningstar_sector_code]
    window_length=1
    
    def compute(self, today, assets, out, sector):
        out[:] = sector[-1]   
        
        
def make_pipeline():
    """
    Create our pipeline.
    """
    
    pricing=USEquityPricing.close.latest

    # Volatility filter (I made it sector neutral to replicate what UBS did).  Uncomment and
    # change the percentile bounds as you would like before adding to 'universe'
    # vol=Volatility(mask=Q500US())
    # sector=morningstar.asset_classification.morningstar_sector_code.latest
    # vol=vol.zscore(groupby=sector)
    # vol_filter=vol.percentile_between(0,100)

    # Liquidity filter (Uncomment and change the percentile bounds as you would like before
    # adding to 'universe'
    # liquidity=Liquidity(mask=Q500US())
    # I included NaN in liquidity filter because of the large amount of missing data for shares out
    # liquidity_filter=liquidity.percentile_between(0,75) | liquidity.isnan()
    
    universe = (
        Q500US()
        & (pricing > 5)
        # & liquidity_filter
        # & volatility_filter
    )


    return Pipeline(
        screen=universe
    )

# before_trading_start is called once per day before the market opens and requires context and data as input. It is frequently used when selecting securities to order.
def before_trading_start(context, data):
    # Gets our pipeline output every day.
    context.output = pipeline_output('my_pipeline')
       

def get_prices(context, data):
    # Get the last 6 days of prices for every stock in our universe
    Universe500=context.output.index.tolist()
    prices = data.history(Universe500,'price',6,'1d')
    daily_rets=np.log(prices/prices.shift(1))

    rets=(prices.iloc[-2] - prices.iloc[0]) / prices.iloc[0]
    # I used data.history instead of Pipeline to get historical prices so you can have the 
    # option of using the intraday price just before the close to get the most recent return.
    # In my post, I argue that you generally get better results when you skip that return.
    # If you don't want to skip the most recent return, however, use .iloc[-1] instead of .iloc[-2]:
    # rets=(prices.iloc[-1] - prices.iloc[0]) / prices.iloc[0]
    
    stdevs=daily_rets.std(axis=0)

    rets_df=pd.DataFrame(rets,columns=['five_day_ret'])
    stdevs_df=pd.DataFrame(stdevs,columns=['stdev_ret'])
    
    context.output=context.output.join(rets_df,how='outer')
    context.output=context.output.join(stdevs_df,how='outer')
    
    context.output['ret_quantile']=pd.qcut(context.output['five_day_ret'],context.nq,labels=False)+1
    context.output['stdev_quantile']=pd.qcut(context.output['stdev_ret'],3,labels=False)+1

    context.longs=context.output[(context.output['ret_quantile']==1) & 
                                (context.output['stdev_quantile']<context.nq_vol)].index.tolist()
    context.shorts=context.output[(context.output['ret_quantile']==context.nq) & 
                                 (context.output['stdev_quantile']<context.nq_vol)].index.tolist()    

    
def my_rebalance(context, data):
    """
    Rebalance daily.
    """
    Universe500=context.output.index.tolist()


    existing_longs=0
    existing_shorts=0
    for security in context.portfolio.positions:
        # Unwind stocks that have moved out of Q500US
        if security not in Universe500 and data.can_trade(security): 
            order_target_percent(security, 0)
        else:
            if data.can_trade(security):
                current_quantile=context.output['ret_quantile'].loc[security]
                if context.portfolio.positions[security].amount>0:
                    if (current_quantile==1) and (security not in context.longs):
                        existing_longs += 1
                    elif (current_quantile>1) and (security not in context.shorts):
                        order_target_percent(security, 0)
                elif context.portfolio.positions[security].amount<0:
                    if (current_quantile==context.nq) and (security not in context.shorts):
                        existing_shorts += 1
                    elif (current_quantile<context.nq) and (security not in context.longs):
                        order_target_percent(security, 0)

    for security in context.longs:
        if data.can_trade(security):
            order_target_percent(security, .5/(len(context.longs)+existing_longs))

    for security in context.shorts:
        if data.can_trade(security):
            order_target_percent(security, -.5/(len(context.shorts)+existing_shorts))


def my_record_vars(context, data):
    """
    Record variables at the end of each day..
    """
    longs = shorts = 0
    for position in context.portfolio.positions.itervalues():
        if position.amount > 0:
            longs += 1
        elif position.amount < 0:
            shorts += 1
    # Record our variables.
    record(leverage=context.account.leverage, long_count=longs, short_count=shorts)
    
    log.info("Today's shorts: "  +", ".join([short_.symbol for short_ in context.shorts]))
    log.info("Today's longs: "  +", ".join([long_.symbol for long_ in context.longs]))