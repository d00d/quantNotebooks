"""
06.27.2017//01: 50MA / 200MA Crossover (SPY)
"""

def initialize(context):
    
    #context.security = symbol('SPY')
    context.asset = sid(8554)
    
   
def handle_data(context,data):
    """
    Called every minute.
    """
    #print(data)
    
    # depreciated
    #MA1 = data.[context.security].mavg(50)    
    ph1 = data.history(context.asset, 'price', 50, '1d')
    MA1 = ph1.mean()

    # depreciated
    #MA2 = data[context.security].mavg(200)
    ph2 = data.history(context.asset, 'price', 200, '1d')
    MA2 = ph2.mean()
    
    # depreciated
    #current_price = data[context.asset].price
    current_price = data.current(context.asset, 'price')
    current_positions = context.portfolio.positions[symbol('SPY')].amount
    cash = context.portfolio.cash
       
    
    if (MA1 > MA2) and current_positions == 0:
        number_of_shares = int(cash/current_price)
        order(context.asset, number_of_shares)
        log.info('Buying Shares')
        
    elif (MA1 < MA2) and current_positions != 0:
        #order(context.security, -current_positions)
        order_target(context.asset, 0)
        log.info('Selling Shares')
        
    record(MA1 = MA1, MA2 = MA2, Price = current_price)


