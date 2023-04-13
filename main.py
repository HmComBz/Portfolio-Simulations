
import datetime
import math
import matplotlib.pyplot as plt
import pandas as pd
import random
import sys
import time
import numpy as np
from borsdata_sdk import BorsdataAPI
from datetime import datetime, timedelta
from matplotlib.figure import Figure

"""
    RUN script as following: python main.py 2010 2022 Custom 45 100
    Start year                  Ex. 2010
    End year                    Ex. 2022
    Type:                       Random or Custom
    Number of stocks            Ex. 45
    Number of simulations       Ex. 500

"""

DATA_PATH = "C:\\borsdata-analys\\Data\\"
PORTFOLIO_LIST = [
    "Adobe Inc", 
    "Advanced Micro Devices Inc", 
    "Alfen NV",
    "Allianz SE",
    "Alphabet Inc",
    "Ansys Inc",
    "AQ Group",
    "ASML Holding NV",
    "Atlas Copco B",
    "Aviva PLC",
    "Bahnhof",
    "Beijer Ref",
    "Boliden",
    "Bonava B",
    "CrowdStrike Holdings Inc",
    "Epiroc B",
    "Fenix Outdoor",
    "Fortinet Inc",
    "Hexagon",
    "Hexatronic",
    "HMS Networks",
    "Infineon Technologies AG",
    "Inwido",
    "Microsoft Corp",
    "NIBE Industrier",
    "Nordic Semiconductor",
    "Novotek",
    "Peab",
    "Plejd",
    "Scanfil",
    "Schneider Electric SE",
    "SEB A",
    "Smart Eye",
    "Snowflake Inc",
    "SolarEdge Technologies Inc",
    "Synopsys Inc",
    "Taiwan Semiconductor",
    "Thule",
    "Troax Group",
    "Umicore SA",
    "Veeva Systems Inc",
    "Vestas Wind Systems",
    "Waystream",
    "Zscaler Inc"
]


############################################################################################
class Portfolio():
    def __init__(self, portfolio_type, num_stocks):
        # Portfolio
        if portfolio_type == "Custom":
            self.portfolio_list = self.select_custom_stocks()
        elif portfolio_type == "Random":
            self.portfolio_list = self.select_random_stocks(num_stocks)
        self.num_stocks = len(self.portfolio_list)
        self.cash_balance = 1500000
        self.portfolio_balance = 0
        self.start_value = self.cash_balance
        self.stocks = self.portfolio_list['Name'].tolist()
        self.stock_ids = self.portfolio_list['ID'].tolist()

        # Create the portfolio
        self.portfolio = dict(zip(self.stock_ids, self.create_portfolio()))

    #---------------------------------------------------------------------------------------
    def buy_stock(self, stock_id, num_companies, stock_price, year, simID):
        # Update stock
        self.portfolio[stock_id]["Total"] = (self.cash_balance + self.portfolio_balance) / num_companies
        self.portfolio[stock_id]["StockPrice"] = stock_price
        self.portfolio[stock_id]["NumStocks"] = self.portfolio[stock_id]["Total"] / stock_price
        self.portfolio[stock_id]["Year"] = year
        self.portfolio[stock_id]["SimID"] = simID
  
        # Update cash balance
        self.cash_balance = self.cash_balance - self.portfolio[stock_id]["Total"]
        self.portfolio_balance = self.portfolio_balance + self.portfolio[stock_id]["Total"]

    #---------------------------------------------------------------------------------------
    def calculate_new_total(self, stock_id):
        self.portfolio[stock_id]["Total"] = self.portfolio[stock_id]["StockPrice"] * self.portfolio[stock_id]["NumStocks"]

    #---------------------------------------------------------------------------------------
    def calculate_annual_profits(self, start_year, end_year):
        year_diff = (end_year - start_year)
        return math.exp(np.log(self.portfolio_balance / self.start_value) / year_diff)

    #---------------------------------------------------------------------------------------
    def create_portfolio(self):
        temp_list = []
        for i in range(0, self.num_stocks):
            temp_row = {"Year":0, "NumStocks":0, "StockPrice":0, "Total":0}
            temp_list.append(temp_row)
        return temp_list

    #---------------------------------------------------------------------------------------
    def select_custom_stocks(self):
        # Select stocks based on users selection
        portfolio_list = pd.read_csv("%sinstruments.csv" % DATA_PATH)
        portfolio_list = portfolio_list[portfolio_list["Name"].isin(PORTFOLIO_LIST)]
        portfolio_list = portfolio_list.drop_duplicates(subset=["Name"], keep="first")
        return portfolio_list

    #---------------------------------------------------------------------------------------
    def select_random_stocks(self, num_stocks):
        # Select random stocks
        portfolio_list = pd.read_csv("%sinstruments.csv" % DATA_PATH)
        list_of_stock_ids = portfolio_list["ID"].tolist()
        random_selection = random.sample(list_of_stock_ids, num_stocks)
        portfolio_list = portfolio_list[portfolio_list["ID"].isin(random_selection)]
        return portfolio_list

    #---------------------------------------------------------------------------------------
    def sell_stock(self, stock_id, stock_price):
        # Update cash balance
        self.cash_balance = self.cash_balance + self.portfolio[stock_id]["Total"]
        self.portfolio_balance = self.portfolio_balance - self.portfolio[stock_id]["Total"]

        # Update stock
        self.portfolio[stock_id]["Total"] = 0
        self.portfolio[stock_id]["NumStocks"] = 0

    #---------------------------------------------------------------------------------------
    def update_portfolio(self, stock_id, stock_price):
        # Update portfolio with new stock price and update total
        self.portfolio[stock_id]["StockPrice"] = stock_price
        self.portfolio[stock_id]["Total"] = self.portfolio[stock_id]["StockPrice"] * self.portfolio[stock_id]["NumStocks"]

    #---------------------------------------------------------------------------------------
    def update_portfolio_balance(self):
        # Update portfolio balance
        total_value = 0
        for stock in self.stock_ids:
            total_value += self.portfolio[stock]["Total"]
        self.portfolio_balance = total_value

    #---------------------------------------------------------------------------------------
    def update_price(self, stock_id, new_price):
        self.portfolio[stock_id]["StockPrice"] = new_price


############################################################################################
class SimulateNonWeighted():
    def __init__(self, start_year, end_year, portfolio_type, num_stocks, num_simulations):
        # Create portfolio object
        portfolio = Portfolio(portfolio_type, num_stocks)

        # Import stock prices
        stock_prices = pd.read_csv("%sstockprices.csv" % DATA_PATH)
        stock_prices["Date"] = stock_prices["Date"].astype('datetime64[ns]')
        stock_prices["Year"] = stock_prices["Date"].dt.strftime("%Y").astype("int")

        # Format dataset to one row per stock and year end
        new_dataframe = pd.DataFrame(columns=["ID", "Date", "ClosePrice", "Year"])
        for stock in portfolio.stock_ids:
            selected_stock_prices = stock_prices[(stock_prices["ID"] == stock) & (stock_prices["Year"].isin([start_year, end_year]))]
            selected_stock_prices_group = selected_stock_prices.groupby(by=["Year"]).agg(
                ID=("ID", "last"),
                Date=("Date", "last"),
                ClosePrice=("Close", "last")
            ).reset_index()
            new_dataframe = new_dataframe.append(selected_stock_prices_group, ignore_index=True)

        # Get list of companies
        prices_start_year = new_dataframe[new_dataframe["Year"] == start_year]
        stock_ids = prices_start_year["ID"].unique().tolist()
        num_companies = len(stock_ids)

        # Buy stocks
        for stock_id in stock_ids:
            stock_price = prices_start_year[prices_start_year["ID"] == stock_id]["ClosePrice"].values[0]
            portfolio.buy_stock(stock_id, num_companies, stock_price)

        # Update portfolio with new values
        prices_end_year = new_dataframe[new_dataframe["Year"] == end_year]
        for stock_id in stock_ids:
            stock_price = prices_end_year[prices_end_year["ID"] == stock_id]["ClosePrice"].values[0]
            portfolio.update_portfolio(stock_id, stock_price)
        
        # Update portfolio balance
        portfolio.update_portfolio_balance()

        # Print results
        print("########################")
        print("##### NOT WEIGHTED #####")
        print("########################")
        print("Portfolio value: %s" % portfolio.portfolio_balance)
        print("Annual growth: %s" % portfolio.calculate_annual_profits(start_year, end_year))
        print("########################")

############################################################################################
class SimulateEquallyWeighted():
    def __init__(self, start_year, end_year, portfolio_type, num_stocks, num_simulations):
        self.results = []
        self.portfolios = pd.DataFrame(columns=["SimID", "NumStocks", "StockPrice", "Total", "Year"])

        # Import stock prices
        stock_prices = pd.read_csv("%sstockprices.csv" % DATA_PATH)
        stock_prices["Date"] = stock_prices["Date"].astype('datetime64[ns]')
        stock_prices["Year"] = stock_prices["Date"].dt.strftime("%Y").astype("int")

        # Create portfolio object
        if portfolio_type == "Random":
            # Run simulation
            file_names = ["final_results.csv", "portfolio.csv"]
            for i in range(0, num_simulations):
                portfolio = Portfolio(portfolio_type, num_stocks)
                self.simulate(stock_prices, portfolio, start_year, end_year, i)
                
        elif portfolio_type == "Custom":
            # Run simulation
            file_names = ["final_results_custom.csv", "portfolio_custom.csv"]
            portfolio = Portfolio(portfolio_type, num_stocks)
            self.simulate(stock_prices, portfolio, start_year, end_year, 0)

        # Save results
        final_results = pd.DataFrame(self.results, columns=["sim_num", "total", "return_pct"])
        avg_capital = final_results[final_results["return_pct"] < 1.4]["total"].mean()
        avg_return = final_results[final_results["return_pct"] < 1.4]["return_pct"].mean()
        median_return = final_results[final_results["return_pct"] < 1.4]["return_pct"].median()
        min_return = final_results[final_results["return_pct"] < 1.4]["return_pct"].min()
        max_return = final_results[final_results["return_pct"] < 1.4]["return_pct"].max()
        print("Avg. capital: %s,   Avg. return: %s,   Median return: %s,   Min return: %s,   Max return: %s" % (avg_capital, avg_return, median_return, min_return, max_return))
        final_results.to_csv(file_names[0], decimal=",", index=False)
        self.portfolios.to_csv(file_names[1], decimal=",", index=False)

        # Visualize
        self.visualize()

    #---------------------------------------------------------------------------------------
    def print_results(self):
        # Print results
        print("########################")
        print("#### EQUAL WEIGHTED ####")
        print("########################")
        print("Portfolio value: %s" % portfolio.portfolio_balance)
        print("Annual growth: %s" % portfolio.calculate_annual_profits(start_year, end_year))
        print("########################")

    #---------------------------------------------------------------------------------------
    def simulate(self, stock_prices, portfolio, start_year, end_year, n):
        # Format dataset to one row per stock and year end
        new_dataframe = pd.DataFrame(columns=["ID", "Date", "ClosePrice", "Year"])
        for stock in portfolio.stock_ids:
            selected_stock_prices = stock_prices[(stock_prices["ID"] == stock) & (stock_prices["Year"] >= start_year) & (stock_prices["Year"] <= end_year)]
            selected_stock_prices_group = selected_stock_prices.groupby(by=["Year"]).agg(
                ID=("ID", "last"),
                Date=("Date", "last"),
                ClosePrice=("Close", "last")
            ).reset_index()
            new_dataframe = new_dataframe.append(selected_stock_prices_group, ignore_index=True)
        
        # Start trading
        new_dataframe = new_dataframe.sort_values(by=["Year"])
        year_list = new_dataframe["Year"].unique().tolist()
        count = 0
        year_balances = []
        for year in year_list:
            prices_curr_year = new_dataframe[new_dataframe["Year"] == year]
            stock_ids = prices_curr_year["ID"].unique().tolist()
            num_companies = len(stock_ids)

            # Update portfolio with new values
            for stock_id in stock_ids:
                stock_price = prices_curr_year[prices_curr_year["ID"] == stock_id]["ClosePrice"].values[0]
                if count > 0:
                    portfolio.update_portfolio(stock_id, stock_price)
            
            # Update portfolio balance
            portfolio.update_portfolio_balance()
            year_balances.append(portfolio.portfolio_balance)

            # Sell stocks
            for stock_id in stock_ids:
                stock_price = prices_curr_year[prices_curr_year["ID"] == stock_id]["ClosePrice"].values[0]
                portfolio.sell_stock(stock_id, stock_price)

            # Buy stocks
            for stock_id in stock_ids:
                stock_price = prices_curr_year[prices_curr_year["ID"] == stock_id]["ClosePrice"].values[0]
                portfolio.buy_stock(stock_id, num_companies, stock_price, year, n)
            count += 1

            # Save portfolio
            temp_portfolio = pd.DataFrame.from_dict(portfolio.portfolio, orient='index')
            temp_portfolio = temp_portfolio.reset_index()
            self.portfolios = self.portfolios.append(temp_portfolio, ignore_index=True)
        
        # Save results
        total = portfolio.portfolio_balance
        results_pct = portfolio.calculate_annual_profits(start_year, end_year)
        self.results.append({"sim_num":n, "total":total, "return_pct":results_pct})

        # Update log
        print("Simulation nr %s:  Total: %s   Results: %s" % (n, total, results_pct))

    #---------------------------------------------------------------------------------------
    def visualize(self):
        # Create lists
        totals = []
        results_pct = []
        for res in self.results:
            totals.append(res["total"])
            results_pct.append(res["return_pct"])

        # Visualise results
        plt.plot(results_pct)
        plt.show()

#########################################
# Run Simulation
#########################################
# Start_date, End_date, Portfolio_Type, Size, Num_Simulations
#SimulateNonWeighted(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], int(sys.argv[4]), int(sys.argv[5]))
SimulateEquallyWeighted(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], int(sys.argv[4]), int(sys.argv[5]))