# Standard Deviation Using History
# Use history() to calculate the standard deviation of the days' closing
# prices of the last 10 trading days, including price at the time of 
# calculation.
def initialize(context):
    # AAPL
    context.aapl = sid(24)

    schedule_function(get_history, date_rules.every_day(), time_rules.market_close())

def get_history(context, data):
    # use history to pull the last 10 days of price
    price_history = data.history(context.aapl, fields='price', bar_count=10, frequency='1d')
    # calculate the standard deviation using std()
    std = price_history.std()
    # record the standard deviation as a custom signal
    record(std=std)
