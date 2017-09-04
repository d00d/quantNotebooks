"""
This algorithm is a good example of a long-short equity
strategy that dynamically selects a portfolio by ranking
securities by multiple custom factors. Algorithms like
these are good candidates for the Quantopian Open.

For background on the theory behind this strategy, refer
to these lectures in the Quantopian Lecture Series
(https://www.quantopian.com/lectures):
    - Lesson 17: Long-Short Equity
    - Lesson 18: Ranking Universes by Factors
"""

from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline import CustomFactor
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.data import morningstar


# Create custom factor #1 Trading Volume/Shares Outstanding
class Factor1(CustomFactor):   
    """
    # Pre-declare inputs and window_length
    inputs = [USEquityPricing.volume, morningstar.valuation.shares_outstanding] 
    window_length = 1
    
    # Compute factor1 value
    def compute(self, today, assets, out, volume, shares):       
        out[:] = volume[-1]/shares[-1]"""
    #calculate NCAV/Price
    
    inputs = [morningstar.valuation.shares_outstanding,
              morningstar.balance_sheet.current_assets,
              morningstar.balance_sheet.total_liabilities,
              morningstar.valuation.market_cap,
              
             ]
    
    
    window_length = 1
    
    def compute(self, today, assets, out, shares_outstanding, current_assets, total_liabilities,market_cap):
        
        out[:] =  (((current_assets[-1] - total_liabilities[-1])/ shares_outstanding[-1])/(market_cap[-1]/shares_outstanding[-1]))

# Create custom factor #2 Price of current day / Price of 60 days ago.        
class Factor2(CustomFactor):   
    """ 
    # Pre-declare inputs and window_length
    inputs = [USEquityPricing.close] 
    window_length = 60
    
    # Compute factor2 value
    def compute(self, today, assets, out, close):       
        out[:] = close[-1]/close[0]
        """
    #calculate z score
    inputs = [morningstar.balance_sheet.working_capital,
              morningstar.balance_sheet.total_assets,
              morningstar.balance_sheet.retained_earnings,
              morningstar.balance_sheet.total_liabilities,
              morningstar.valuation.market_cap,
              morningstar.income_statement.ebit,
              morningstar.cash_flow_statement.domestic_sales,
              morningstar.cash_flow_statement.foreign_sales,
              
             ]
    
    
    window_length = 1
    
    def compute(self, today, assets, out, working_capital, total_assets, retained_earnings, total_liabilities,market_cap, ebit, domestic_sales,foreign_sales):
        
        out[:] = (1 /  ((1.2*(working_capital[-1]/total_assets[-1]))+(1.4*(retained_earnings[-1]/total_assets[-1]))+ (3.3*(ebit[-1]/total_assets[-1]))+ (.6*(market_cap[-1]/total_liabilities[-1]))+(.999*((domestic_sales[-1]+foreign_sales[-1])/total_assets[-1]))))


    
# Create custom factor to calculate a market cap based on yesterday's close
# We'll use this to get the top 2000 stocks by market cap
class MarketCap(CustomFactor):   
    
    # Pre-declare inputs and window_length
    inputs = [USEquityPricing.close, morningstar.valuation.shares_outstanding] 
    window_length = 1
    
    # Compute market cap value
    def compute(self, today, assets, out, close, shares):       
        out[:] = close[-1] * shares[-1]

def initialize(context):
    
    context.long_leverage = 0.50
    context.short_leverage = -0.50
    
    pipe = Pipeline()
    attach_pipeline(pipe, 'ranked_2000')
    
    #add the two factors defined to the pipeline
    factor1 = Factor1()
    pipe.add(factor1, 'factor_1') 
    factor2 = Factor2()
    pipe.add(factor2, 'factor_2')
    
    # Create and apply a filter representing the top 2000 equities by MarketCap every day
    # This is an approximation of the Russell 2000
    
    mkt_cap = MarketCap()
    top_2000 = mkt_cap.top(2000)
    pipe.set_screen(top_2000)
    
    # Rank factor 1 and add the rank to our pipeline
    factor1_rank = factor1.rank(mask=top_2000)
    pipe.add(factor1_rank, 'f1_rank')
    # Rank factor 2 and add the rank to our pipeline
    factor2_rank = factor2.rank(mask=top_2000)
    pipe.add(factor2_rank, 'f2_rank')
    # Take the average of the two factor rankings, add this to the pipeline
    combo_raw = (factor1_rank+factor2_rank)
    pipe.add(combo_raw, 'combo_raw') 
    # Rank the combo_raw and add that to the pipeline
    pipe.add(combo_raw.rank(mask=top_2000), 'combo_rank')      
            
    # Scedule my rebalance function
    schedule_function(func=rebalance, 
                      date_rule=date_rules.month_start(days_offset=0), 
                      time_rule=time_rules.market_open(hours=0,minutes=30), 
                      half_days=True)
    
            
def before_trading_start(context, data):
    # Call pipelive_output to get the output
    # Note this is a dataframe where the index is the SIDs for all securities to pass my screen
    # and the colums are the factors which I added to the pipeline
    context.output = pipeline_output('ranked_2000')
    #there are some NaNs in factor 2, I'm removing those
    ranked_2000 = context.output.fillna(0)
    ranked_2000 = context.output[context.output.factor_2 > 0]
    ranked_2000 = context.output[context.output.factor_1 > 0]
      
    # Narrow down the securities to only the top 500 & update my universe
    context.short_list = ranked_2000.sort(['combo_rank'], ascending=False).iloc[:50]
    context.long_list = ranked_2000.sort(['combo_rank'], ascending=False).iloc[-50:]   
    
    update_universe(context.long_list.index.union(context.short_list.index)) 


def handle_data(context, data):  
    
     # Record and plot the leverage of our portfolio over time. 
    record(leverage = context.account.leverage)
    
    print "Long List"
    log.info("\n" + str(context.long_list.sort(['combo_rank'], ascending=True).head(10)))
    
    print "Short List" 
    log.info("\n" + str(context.short_list.sort(['combo_rank'], ascending=False).head(10)))

# This rebalancing is called according to our schedule_function settings.     
def rebalance(context,data):
    
    long_weight = context.long_leverage / float(len(context.long_list))
    short_weight = context.short_leverage / float(len(context.short_list))

    
    for long_stock in context.long_list.index:
        if long_stock in data:
            log.info("ordering longs")
            log.info("weight is %s" % (long_weight))
            order_target_percent(long_stock, long_weight)
        
    for short_stock in context.short_list.index:
        if short_stock in data:
            log.info("ordering shorts")
            log.info("weight is %s" % (short_weight))
            order_target_percent(short_stock, -short_weight)
        
    for stock in context.portfolio.positions.iterkeys():
        if stock not in context.long_list.index and stock not in context.short_list.index:
            order_target(stock, 0)
