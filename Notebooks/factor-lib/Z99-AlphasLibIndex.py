bs = morningstar.balance_sheet
cfs = morningstar.cash_flow_statement
is_ = morningstar.income_statement
or_ = morningstar.operation_ratios
er = morningstar.earnings_report
v = morningstar.valuation
vr = morningstar.valuation_ratios

def make_factors():
    """List of many factors for use in cross-sectional factor algorithms"""   
    
    """TRADITIONAL VALUE"""

    def Price_To_Sales():
        """
        Price to Sales Ratio:
        Closing price divided by sales per share.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        Low P/S Ratio suggests that an equity cheap
        Differs substantially between sectors
        """
        return vr.ps_ratio.latest

    def Price_To_Earnings():
        """
        Price to Earnings Ratio:
        Closing price divided by earnings per share.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        Low P/E Ratio suggests that an equity cheap
        Differs substantially between sectors
        """
        return vr.pe_ratio.latest

    def Price_To_Diluted_Earnings():
        """
        Price to Diluted Earnings Ratio:
        Closing price divided by diluted earnings per share.
        Diluted Earnings include dilutive securities
        Options, convertible bonds etc.)
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        Low P/Diluted Earnings Ratio suggests that equity is cheap
        Differs substantially between sectors
        """
        return USEquityPricing.close.latest / \
            (er.diluted_eps.latest * 4.)

    def Price_To_Forward_Earnings():
        """
        Price to Forward Earnings Ratio:
        Closing price divided by projected earnings for
        next fiscal period.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        Low P/FY1 EPS Ratio suggests that equity is cheap
        Differs substantially between sectors
        """
        return vr.forward_pe_ratio.latest

    def Dividend_Yield():
        """
        Dividend Yield:
        Dividends per share divided by closing price.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High Dividend Yield Ratio suggests that an equity
        is attractive to an investor as the dividends
        paid out will be a larger proportion of
        the price they paid for it.
        """
        return vr.dividend_yield.latest

    def Price_To_Free_Cashflows():
        """
        Price to Free Cash Flows:
        Closing price divided by free cash flow.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        Low P/ Free Cash Flows suggests that equity is cheap
        Differs substantially between sectors
        """
        return USEquityPricing.close.latest / \
            vr.fcf_per_share.latest

    def Price_To_Operating_Cashflows():
        """
        Price to Operating Cash Flows:
        Closing price divided by operating cash flow.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        Low P/ Operating Cash Flows suggests that equity is cheap
        Differs substantially between sectors
        """
        return USEquityPricing.close.latest / \
            vr.cfo_per_share.latest

    def Price_To_Book():
        """
        Price to Book Value:
        Closing price divided by book value.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        Low P/B Ratio suggests that equity is cheap
        Differs substantially between sectors
        """
        return USEquityPricing.close.latest / \
            vr.book_value_per_share.latest

    def Cashflows_To_Assets():
        """
        Cash flows to Assets:
        Operating Cash Flows divided by total assets.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High Cash Flows to Assets Ratio suggests that the
        company has cash for future operations
        """
        return vr.cfo_per_share.latest / \
            (bs.total_assets.latest / v.shares_outstanding.latest)

    def EV_To_Cashflows():
        """
        Enterprise Value to Cash Flows:
        Enterprise Value divided by Free Cash Flows.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        Low EV/FCF suggests that a company has a good amount of
        money relative to its size readily available
        """
        return v.enterprise_value.latest / \
            cfs.free_cash_flow.latest

    def EV_To_EBITDA():
        """
        Enterprise Value to Earnings Before Interest, Taxes,
        Deprecation and Amortization (EBITDA):
        Enterprise Value divided by EBITDA.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        Low EV/EBITDA suggests that equity is cheap
        Differs substantially between sectors / companies
        """
        return v.enterprise_value.latest / \
            (is_.ebitda.latest * 4.)

    def EBITDA_Yield():
        """
        EBITDA Yield:
        EBITDA divided by close price.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High EBITDA Yield suggests that a company is profitable
        """
        return (is_.ebitda.latest * 4.) / \
            USEquityPricing.close.latest
        
    """SIZE"""
    def Market_Cap():
        """
        Market Capitalization:
        Market Capitalization of the company issuing the equity.
        (Close Price * Shares Outstanding)
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value for large companies, low value for small companies
        """
        return morningstar.valuation.market_cap.latest

    # Log Market Cap
    def Log_Market_Cap():
        """
        Natural Logarithm of Market Capitalization:
        Log of Market Cap. log(Close Price * Shares Outstanding)
        https://www.math.nyu.edu/faculty/avellane/Lo13030.pdf
        Notes:
        High value for large companies, low value for small companies
        Limits the outlier effect of very large companies through
        log transformation
        """
        return morningstar.valuation.market_cap.latest.log()

    def Log_Market_Cap_Cubed():
        """
        Natural Logarithm of Market Capitalization Cubed:
        Log of Market Cap Cubed.
        https://www.math.nyu.edu/faculty/avellane/Lo13030.pdf
        Notes:
        High value for large companies, low value for small companies
        Limits the outlier effect of very large companies through
        log transformation
        """
        return morningstar.valuation.market_cap.latest.log() ** 3

    
    """MOMENTUM"""

    class Price_Oscillator(CustomFactor):
        """
        4/52-Week Price Oscillator:
        Average close prices over 4-weeks divided by average close
        prices over 52-weeks all less 1.
        https://www.math.nyu.edu/faculty/avellane/Lo13030.pdf
        Notes:
        High value suggests momentum
        """
        inputs = [USEquityPricing.close]
        window_length = 252

        def compute(self, today, assets, out, close):
            four_week_period = close[-20:]
            out[:] = (np.nanmean(four_week_period, axis=0) /
                      np.nanmean(close, axis=0)) - 1.

    class Trendline(CustomFactor):
        """
        52-Week Trendline:
        Slope of the linear regression across a 1 year lookback window.
        https://www.math.nyu.edu/faculty/avellane/Lo13030.pdf
        Notes:
        High value suggests momentum
        Calculated using the MLE of the slope of the regression
        """
        inputs = [USEquityPricing.close]
        window_length = 252

        # using MLE for speed
        def compute(self, today, assets, out, close):

            # prepare X matrix (x_is - x_bar)
            X = range(self.window_length)
            X_bar = np.nanmean(X)
            X_vector = X - X_bar
            X_matrix = np.tile(X_vector, (len(close.T), 1)).T

            # prepare Y matrix (y_is - y_bar)
            Y_bar = np.nanmean(close, axis=0)
            Y_bars = np.tile(Y_bar, (self.window_length, 1))
            Y_matrix = close - Y_bars

            # prepare variance of X
            X_var = np.nanvar(X)

            # multiply X matrix an Y matrix and sum (dot product)
            # then divide by variance of X
            # this gives the MLE of Beta
            out[:] = (np.sum((X_matrix * Y_matrix), axis=0) / X_var) / \
                (self.window_length)

    def Price_Momentum_1M():
        """
        1-Month Price Momentum:
        1-month closing price rate of change.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests momentum (shorter term)
        Equivalent to analysis of returns (1-month window)
        """
        return Returns(window_length=21)

    def Price_Momentum_3M():
        """
        3-Month Price Momentum:
        3-month closing price rate of change.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests momentum (shorter term)
        Equivalent to analysis of returns (3-month window)
        """
        return Returns(window_length=63)

    def Price_Momentum_6M():
        """
        6-Month Price Momentum:
        6-month closing price rate of change.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests momentum (medium term)
        Equivalent to analysis of returns (6-month window)
        """
        return Returns(window_length=126)

    def Price_Momentum_12M():
        """
        12-Month Price Momentum:
        12-month closing price rate of change.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests momentum (long term)
        Equivalent to analysis of returns (12-month window)
        """
        return Returns(window_length=252)

    def Returns_39W():
        """
        39-Week Returns:
        Returns over 39-week window.
        https://www.math.nyu.edu/faculty/avellane/Lo13030.pdf
        Notes:
        High value suggests momentum (long term)
        Equivalent to analysis of price momentum (39-week window)
        """
        return Returns(window_length=215)

    class Mean_Reversion_1M(CustomFactor):
        """
        1-Month Mean Reversion:
        1-month return less 12-month average of monthly return, all over
        standard deviation of 12-month average of monthly returns.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests momentum (short term)
        Equivalent to analysis of returns (12-month window)
        """
        inputs = [Returns(window_length=21)]
        window_length = 252

        def compute(self, today, assets, out, monthly_rets):
            out[:] = (monthly_rets[-1] - np.nanmean(monthly_rets, axis=0)) / \
                np.nanstd(monthly_rets, axis=0)
                
    """EFFICIENCY"""
    
    def Capex_To_Assets():
        """
        Capital Expnditure to Assets:
        Capital Expenditure divided by Total Assets.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests good efficiency, as expenditure is
        being used to generate more assets
        """
        return (cfs.capital_expenditure.latest * 4.) / \
            bs.total_assets.latest

    def EV_To_Sales_SalesGrowth():
        """
        Enterprise Value to Sales to Sales Growth
        EV divided by Sales divided by Sales Growth.
        
        """
        return  (v.enterprise_value.latest) / \
                (is_.total_revenue.latest * 4.) / \
                Returns(inputs=[is_.total_revenue], window_length=252)
        
    def Capex_To_Sales():
        """
        Capital Expnditure to Sales:
        Capital Expenditure divided by Total Revenue.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests good efficiency, as expenditure is
        being used to generate greater sales figures
        """
        return (cfs.capital_expenditure.latest * 4.) / \
            (is_.total_revenue.latest * 4.)

    def Capex_To_Cashflows():
        """
        Capital Expnditure to Cash Flows:
        Capital Expenditure divided by Free Cash Flows.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests good efficiency, as expenditure is
        being used to generate greater free cash flows
        """
        return (cfs.capital_expenditure.latest * 4.) / \
            (cfs.free_cash_flow.latest * 4.)

    def EBIT_To_Assets():
        """
        Earnings Before Interest and Taxes (EBIT) to Total Assets:
        EBIT divided by Total Assets.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests good efficiency, as earnings are
        being used to generate more assets
        """
        return (is_.ebit.latest * 4.) / \
            bs.total_assets.latest

    def Operating_Cashflows_To_Assets():
        """
        Operating Cash Flows to Total Assets:
        Operating Cash Flows divided by Total Assets.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests good efficiency, as cash being used
        for operations is being used to generate more assets
        """
        return (cfs.operating_cash_flow.latest * 4.) / \
            bs.total_assets.latest

    def Retained_Earnings_To_Assets():
        """
        Retained Earnings to Total Assets:
        Retained Earnings divided by Total Assets.
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests good efficiency, as greater 
        retained earnings are being used to generate more assets
        """
        return bs.retained_earnings.latest / \
            bs.total_assets.latest

    """RISK/SIZE"""

    class Downside_Risk(CustomFactor):
        """
        Downside Risk:
        Standard Deviation of 12-month monthly losses
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests high risk of losses
        """
        inputs = [Returns(window_length=2)]
        window_length = 252

        def compute(self, today, assets, out, rets):
            down_rets = np.where(rets < 0, rets, np.nan)
            out[:] = np.nanstd(down_rets, axis=0)

    def SPY_Beta():
        """
        Index Beta:
        Slope coefficient of 1-year regression of price returns
        against index returns
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests high market risk
        Slope calculated using regression MLE
        """
        return RollingLinearRegressionOfReturns(
            target=symbols('SPY'),
            # above for research
            # target=sid(8554) for backtester
            returns_length=2,
            regression_length=252
        ).beta

    class Vol_3M(CustomFactor):
        """
        3-month Volatility:
        Standard deviation of returns over 3 months
        http://www.morningstar.com/invglossary/historical_volatility.aspx
        Notes:
        High Value suggests that equity price fluctuates wildly
        """

        inputs = [Returns(window_length=2)]
        window_length = 63

        def compute(self, today, assets, out, rets):
            out[:] = np.nanstd(rets, axis=0)

    """GROWTH"""

    def Sales_Growth_3M():
        """
        3-month Sales Growth:
        Increase in total sales over 3 months
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value represents large growth (short term)
        """
        return Returns(inputs=[is_.total_revenue], window_length=63)

    def Sales_Growth_12M():
        """
        12-month Sales Growth:
        Increase in total sales over 12 months
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value represents large growth (long term)
        """
        return Returns(inputs=[is_.total_revenue], window_length=252)

    def EPS_Growth_12M():
        """
        12-month Earnings Per Share Growth:
        Increase in EPS over 12 months
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value represents large growth (long term)
        """
        return Returns(inputs=[er.basic_eps], window_length=252)

    """QUALITY"""

    class Asset_Turnover(CustomFactor):
        """
        Asset Turnover:
        Sales divided by average of year beginning and year end assets
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value represents good financial health
        Varies substantially between sectors
        """
        inputs = [is_.total_revenue, bs.total_assets]
        window_length = 252

        def compute(self, today, assets, out, sales, tot_assets):
            out[:] = (sales[-1] * 4.) / \
                ((tot_assets[-1] + tot_assets[0]) / 2.)

    def Asset_Growth_3M():
        """
        3-month Asset Growth:
        Increase in total assets over 3 months
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value represents good financial health as quantity of
        assets is increasing
        """
        return Returns(inputs=[bs.total_assets], window_length=63)

    def Current_Ratio():
        """
        Current Ratio:
        Total current assets divided by total current liabilities
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value represents good financial health as assets
        are greater than liabilities (>1)
        Morningstar built-in fundamental ratio more accurate
        than calculated ratio
        """
        return or_.current_ratio.latest

    def Asset_To_Equity_Ratio():
        """
        Asset / Equity Ratio
        Total current assets divided by common equity
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests that company has taken on substantial debt
        Vaires substantially with industry
        """
        return bs.total_assets.latest / bs.common_stock_equity.latest

    def Interest_Coverage():
        """
        Interest Coverage:
        EBIT divided by interest expense
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests that company has taken on substantial debt
        Varies substantially with industry
        """
        return is_.ebit.latest / is_.interest_expense.latest

    def Debt_To_Asset_Ratio():
        """
        Debt / Asset Ratio:
        Total Debts divided by Total Assets
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests that company has taken on substantial debt
        Low value suggests good financial health as assets greater than debt
        Long Term Debt
        """
        return bs.total_debt.latest / bs.total_assets.latest

    def Debt_To_Equity_Ratio():
        """
        Debt / Equity Ratio:
        Total Debts divided by Common Stock Equity
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests that company is taking on debts to leverage
        Low value suggests good financial health as little-to-no leveraging
        Long Term Debt
        """
        return bs.total_debt.latest / bs.common_stock_equity.latest

    class Net_Debt_Growth_3M(CustomFactor):
        """
        3-Month Net Debt Growth:
        Increase in net debt (total debt - cash) over 3 month window
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests that a company is acquiring more debt
        Low value suggests good financial health as debt is decreasing
        Long Term Debt
        """
        inputs = [bs.total_debt, bs.cash_and_cash_equivalents]
        window_length = 92

        def compute(self, today, assets, out, debt, cash):
            out[:] = ((debt[-1] - cash[-1]) - (debt[0] - cash[0])) / \
                (debt[0] - cash[0])

    def Working_Capital_To_Assets():
        """
        Working Capital / Assets:
        Current Assets less Current liabilities (Working Capital)
        all divided by Assets
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests that company is currently using more money
        than they are saving (holding as assets)
        Low value suggests good financial health as holding more in assets
        than risking through use working_capital more accurate 
        than total_assets and total_liabilities
        """
        return bs.working_capital.latest / bs.total_assets.latest

    def Working_Capital_To_Sales():
        """
        Working Capital / Sales:
        Current Assets less Current liabilities (Working Capital)
        all divided by Sales
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests that company is currently using more money
        than taking in through sales than they are saving (holding as assets)
        Low value suggests good financial health as sales stronger
        than money risked through use
        working_capital more accurate than total_assets and total_liabilities
        """
        return bs.working_capital.latest / is_.total_revenue.latest
    
    
    def Earnings_Quality():
        return morningstar.cash_flow_statement.operating_cash_flow.latest / \
               EarningsSurprises.eps_act.latest
    
    
    """PAYOUT"""

    def Dividend_Growth():
        """
        Dividend Growth:
        Growth in dividends observed over a 1-year lookback window
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests that rate at which the quantity of dividends
        paid out is increasing Morningstar built-in fundamental
        better as adjusts inf values
        """
        return morningstar.earnings_ratios.dps_growth.latest

    def Dividend_Payout_Ratio():
        """
        Dividend Payout Ratio:
        Dividends Per Share divided by Earnings Per Share
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests that the amount of earnings paid back
        to equityholders is large
        """
        return morningstar.earnings_report.dividend_per_share.latest / \
            morningstar.earnings_report.basic_eps.latest
        
        
    """PROFITABILITY"""

    def Gross_Income_Margin():
        """
        Gross Income Margin:
        Gross Profit divided by Net Sales
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests that the company is generating large profits
        """
        return is_.gross_profit.latest / is_.total_revenue.latest

    def Net_Income_Margin():
        """
        Gross Income Margin:
        Gross Profit divided by Net Sales
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests that the company is generating large profits
        Builtin used as cleans inf values
        """
        return or_.net_margin.latest

    def Return_On_Total_Equity():
        """
        Return on Total Equity:
        Net income divided by average of total shareholder equity
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests that the company is generating large profits
        Builtin used as cleans inf values
        """
        return or_.roe.latest

    def Return_On_Total_Assets():
        """
        Return on Total Assets:
        Net income divided by average total assets
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests that the company is generating large profits
        Builtin used as cleans inf values
        """
        return or_.roa.latest

    def Return_On_Total_Invest_Capital():
        """
        Return on Total Invest Capital:
        Net income divided by average total invested capital
        https://www.pnc.com/content/dam/pnc-com/pdf/personal/wealth-investments/WhitePapers/FactorAnalysisFeb2014.pdf # NOQA
        Notes:
        High value suggests that the company is generating large profits
        Builtin used as cleans inf values
        """
        return or_.roic.latest

    """PRICE REVERSAL"""

    # 10 Day MACD signal line
    class MACD_Signal_10d(CustomFactor):
        """
        10 Day Moving Average Convergence/Divergence (MACD) signal line:
        10 day Exponential Moving Average (EMA) of the difference between
        12-day and 26-day EMA of close price
        https://www.math.nyu.edu/faculty/avellane/Lo13030.pdf
        Notes:
        High value suggests turning point in positive momentum
        Low value suggests turning point in negative momentum
        """
        inputs = [USEquityPricing.close]
        window_length = 60

        def compute(self, today, assets, out, close):

            sig_lines = []

            for col in close.T:
                # get signal line only
                try:
                    _, signal_line, _ = talib.MACD(col, fastperiod=12,
                                                   slowperiod=26, signalperiod=10)
                    sig_lines.append(signal_line[-1])
                # if error calculating, return NaN
                except:
                    sig_lines.append(np.nan)
            out[:] = sig_lines

    # 20-day Stochastic Oscillator
    class Stochastic_Oscillator(CustomFactor):
        """
        20-day Stochastic Oscillator:
        K = (close price - 5-day low) / (5-day high - 5-day low)
        D = 100 * (average of past 3 K's)
        We use the slow-D period here (the D above)
        https://www.math.nyu.edu/faculty/avellane/Lo13030.pdf
        Notes:
        High value suggests turning point in positive momentum (expected decrease)
        Low value suggests turning point in negative momentum (expected increase)
        """
        inputs = [USEquityPricing.close,
                  USEquityPricing.high, USEquityPricing.low]
        window_length = 30

        def compute(self, today, assets, out, close, high, low):

            stoch_list = []

            for col_c, col_h, col_l in zip(close.T, high.T, low.T):
                try:
                    _, slowd = talib.STOCH(col_h, col_l, col_c,
                                           fastk_period=5, slowk_period=3, slowk_matype=0,
                                           slowd_period=3, slowd_matype=0)
                    stoch_list.append(slowd[-1])
                # if error calculating
                except:
                    stoch_list.append(np.nan)

            out[:] = stoch_list

    # 5-day Money Flow / Volume
    class Moneyflow_Volume_5d(CustomFactor):
        """
        5-day Money Flow / Volume:
        Numerator: if price today greater than price yesterday, add price * volume, otherwise subtract
        Denominator: prices and volumes multiplied and summed
        https://www.math.nyu.edu/faculty/avellane/Lo13030.pdf
        Notes:
        High value suggests turning point in positive momentum (expected decrease)
        Low value suggests turning point in negative momentum (expected increase)
        """
        inputs = [USEquityPricing.close, USEquityPricing.volume]
        window_length = 5

        def compute(self, today, assets, out, close, volume):

            mfvs = []

            for col_c, col_v in zip(close.T, volume.T):

                # denominator
                denominator = np.dot(col_c, col_v)

                # numerator
                numerator = 0.
                for n, price in enumerate(col_c.tolist()):
                    if price > col_c[n - 1]:
                        numerator += price * col_v[n]
                    else:
                        numerator -= price * col_v[n]

                mfvs.append(numerator / denominator)
            out[:] = mfvs

    """TECHNICALS"""

#     # Merton's Distance to Default
#     class Mertons_DD(CustomFactor):
#         """
#         Merton's Distance to Default:
#         Application the BS Formula with assets as S_t and liabilities as the strike
#         https://www.bostonfed.org/bankinfo/conevent/slowdown/groppetal.pdf
#         Notes:
#         Lower value suggests increased default risk of company issuing equity
#         """
#         inputs = [morningstar.balance_sheet.total_assets,
#                   morningstar.balance_sheet.total_liabilities, libor.value, USEquityPricing.close]
#         window_length = 252

#         def compute(self, today, assets, out, tot_assets, tot_liabilities, r, close):
#             mertons = []

#             for col_assets, col_liabilities, col_r, col_close in zip(tot_assets.T, tot_liabilities.T,
#                                                                      r.T, close.T):
#                 vol_1y = np.nanstd(col_close)
#                 numerator = np.log(
#                     col_assets[-1] / col_liabilities[-1]) + ((252 * col_r[-1]) - ((vol_1y**2) / 2))
#                 mertons.append(numerator / vol_1y)

#             out[:] = mertons
                   
            
    all_factors = {
        'Asset Growth 3M': Asset_Growth_3M,
        'EV_To_Sales_SalesGrowth': EV_To_Sales_SalesGrowth,
        'Asset to Equity Ratio': Asset_To_Equity_Ratio,
        'Asset Turnover': Asset_Turnover,
        'Capex to Assets': Capex_To_Assets,
        'Capex to Cashflows': Capex_To_Cashflows,
        'Capex to Sales': Capex_To_Sales,
        'Cashflows to Assets': Cashflows_To_Assets,
        'Current Ratio': Current_Ratio,
        'Debt to Asset Ratio': Debt_To_Asset_Ratio,
        'Dividend Growth': Dividend_Growth,
        'Dividend Payout Ratio': Dividend_Payout_Ratio,
        'Dividend Yield': Dividend_Yield,
        'EBITDA Yield': EBITDA_Yield,
        'EBIT to Assets': EBIT_To_Assets,
        'Earnings Quality': Earnings_Quality,
        'EV to Cashflows': EV_To_Cashflows,
        'EV to EBITDA': EV_To_EBITDA,
        'Gross Income Margin': Gross_Income_Margin,
        'Interest Coverage': Interest_Coverage,
        'MACD Signal Line': MACD_Signal_10d,
        'Market Cap': Market_Cap,
        'Mean Reversion 1M': Mean_Reversion_1M,
        'Moneyflow Volume 5D': Moneyflow_Volume_5d,
        'Net Debt Growth 3M': Net_Debt_Growth_3M,
        'Net Income Margin': Net_Income_Margin,
        'Operating Cashflows to Assets': Operating_Cashflows_To_Assets,
        'Price Momentum 12M': Price_Momentum_12M,
        'Price Momentum 1M': Price_Momentum_1M,
        'Price Momentum 3M': Price_Momentum_3M,
        'Price Momentum 6M': Price_Momentum_6M,
        'Price Oscillator': Price_Oscillator,
        'Price to Book': Price_To_Book,
        'Price to Diluted Earnings': Price_To_Diluted_Earnings,
        'Price to Earnings': Price_To_Earnings,
        'Price to Forward Earnings': Price_To_Forward_Earnings,
        'Price to Free Cashflows': Price_To_Free_Cashflows,
        'Price to Operating Cashflows': Price_To_Operating_Cashflows,
        'Price to Sales': Price_To_Sales,
        'Retained Earnings to Assets': Retained_Earnings_To_Assets,
        'Return on Total Assets': Return_On_Total_Assets,
        'Return on Total Equity': Return_On_Total_Equity,
        'Return on Invest Capital': Return_On_Total_Invest_Capital,
        '39 Week Returns': Returns_39W,
        'Stochastic Oscillator': Stochastic_Oscillator,
        'Trendline': Trendline,
        'Vol 3M': Vol_3M,
        'Working Capital to Assets': Working_Capital_To_Assets,
        'Working Capital to Sales': Working_Capital_To_Sales,
    }
    
    return all_factors