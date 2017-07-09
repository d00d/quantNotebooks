from quantopian.algorithm import attach_pipeline, pipeline_output  
from quantopian.pipeline import Pipeline  
from quantopian.pipeline import CustomFactor 
from quantopian.pipeline.factors.morningstar import MarketCap
from quantopian.pipeline.data.builtin import USEquityPricing  
from quantopian.pipeline.data import morningstar  
from quantopian.pipeline.classifiers.morningstar import Sector
from quantopian.pipeline.factors import AverageDollarVolume, SimpleMovingAverage
from quantopian.pipeline.filters.morningstar import IsPrimaryShare

# Create Price to Sales Custom Factor  
class Price_to_sales(CustomFactor):  
    # Pre-declare inputs and window_length  
    inputs = [morningstar.valuation_ratios.ps_ratio]  
    window_length = 1  
    # Compute factor1 value  
    def compute(self, today, assets, out, ps_ratio):  
        out[:] = ps_ratio[-1]

def initialize(context):  
    pipe = Pipeline()  
    attach_pipeline(pipe, 'ranked_2000')  
    my_sectors = [206] #"Healthcare"  
    sector_filter = Sector().element_of(my_sectors)  
    # Create list of all criteria for securities worth investing in  

    """
    9 filters:
        1. common stock
        2 & 3. not limited partnership - name and database check
        4. database has fundamental data
        5. not over the counter
        6. not when issued
        7. not depository receipts
        8. primary share
        9. high dollar volume
    """
    mkt_cap = MarketCap()  
    top_2000 = mkt_cap.top(2000)
    common_stock = morningstar.share_class_reference.security_type.latest.eq('ST00000001')
    not_lp_name = ~morningstar.company_reference.standard_name.latest.matches('.* L[\\. ]?P\.?$')
    not_lp_balance_sheet = morningstar.balance_sheet.limited_partnership.latest.isnull()
    have_data = morningstar.valuation.market_cap.latest.notnull()
    not_otc = ~morningstar.share_class_reference.exchange_id.latest.startswith('OTC')
    not_wi = ~morningstar.share_class_reference.symbol.latest.endswith('.WI')
    not_depository = ~morningstar.share_class_reference.is_depositary_receipt.latest
    primary_share = IsPrimaryShare()
    
    # Combine the above filters.
    tradable_filter = (common_stock & not_lp_name & not_lp_balance_sheet &
                       have_data & not_otc & not_wi & not_depository & primary_share & sector_filter & top_2000)
    pipe_screen = (sector_filter)  

    # Create, register and name a pipeline  

    # Add the factor defined to the pipeline  
    price_to_sales = Price_to_sales(mask=tradable_filter)  
    pipe.add(price_to_sales, 'price to sales')  

    # Create and apply a filter representing the top 2000 equities by MarketCap every day  
    # This is an approximation of the Russell 2000  
  
    # Rank price to sales and add the rank to our pipeline  
    price_to_sales_rank = price_to_sales.rank(mask=tradable_filter)  
    pipe.add(price_to_sales_rank, 'ps_rank')  
    # Set a screen to ensure that only the top 2000 companies by market cap  
    # which are healthcare companies with positive PS ratios are screened  
    pipe.set_screen(top_2000 & (price_to_sales>0))  
    # Scedule my rebalance function  
    schedule_function(func=rebalance,  
                      date_rule=date_rules.month_start(days_offset=0),  
                      time_rule=time_rules.market_open(hours=0,minutes=30),  
                      half_days=True)  
    # Schedule my plotting function  
    schedule_function(func=record_vars,  
                      date_rule=date_rules.every_day(),  
                      time_rule=time_rules.market_close(),  
                      half_days=True)  
    # set my leverage  
    context.long_leverage = 1.00  
    context.short_leverage = -0.00 
    context.spy = sid(8554)  
    context.short_list = []
    context.short_list.append(context.spy)
    
    
def before_trading_start(context, data):  
    # Call pipelive_output to get the output  
    context.output = pipeline_output('ranked_2000')  
    # Narrow down the securities to only the 20 with lowest price to sales & update my universe  
    context.long_list = context.output.sort_values(by='ps_rank', ascending=False).iloc[:-20]  
  

def record_vars(context, data):  
     # Record and plot the leverage of our portfolio over time.  
    record(leverage = context.account.leverage)  
    log.info( 'Long List')  
    log.info("\n" + str(context.long_list.sort(['ps_rank'], ascending=True).head(10)))  
    log.info( 'Short List')
    log.info('\n' + str(context.short_list)  )


# This rebalancing is called according to our schedule_function settings.  
def rebalance(context, data):  
    long_weight = context.long_leverage / float(len(context.long_list))  
    short_weight = context.short_leverage / float(len(context.short_list))
    for stock in context.portfolio.positions.iterkeys():  
        if stock not in context.long_list.index and stock not in context.short_list:  
            order_target(stock, 0)
            
    log.info("ordering longs")  
    log.info("weight is %s" % (long_weight))  
    for long_stock in context.long_list.index:  
        order_target_percent(long_stock, long_weight)  
    log.info("ordering shorts")  
    log.info("weight is %s" % (short_weight))  
    for short_stock in context.short_list:  
        order_target_percent(short_stock, short_weight)  
