from borsdata_sdk import BorsdataAPI
import logging
import pandas as pd
import requests
import time
import numpy as np
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

API_KEY = os.getenv("BORSDATA_API")
borsdata = BorsdataAPI(API_KEY)

# Create loggers for code
logger = logging.getLogger("Shelly")
logger.setLevel(logging.INFO)
logger.propagate = False

# Create handler
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)

# Add handler to logger
logger.addHandler(consoleHandler)

# Set formatting to logger
formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')
consoleHandler.setFormatter(formatter)

# GLOBAL LISTS
DATA_PATH = "C:\\borsdata-analys\\Data\\"
PORTFOLIO_LIST = pd.read_csv("%sselected_instruments.csv" % DATA_PATH, encoding="utf-8", delimiter=";")["Bolagsnamn"].tolist()


###########################################################################################
class Import():
    def __init__(self):
        # Get dimensional data
        self.branches = pd.DataFrame(self.get_branches(), columns=["ID", "Name", "SectorID"])
        self.instruments = pd.DataFrame(self.get_instruments(), columns=["ID", "Name", "Sector", "Market", "Bransch", "Country"])
        self.global_instruments = pd.DataFrame(self.get_instruments_global(), columns=["ID", "Name", "Sector", "Market", "Bransch", "Country"])
        self.instruments = self.instruments.append(self.global_instruments, ignore_index=True).sort_values("Name")
        self.markets = pd.DataFrame(self.get_markets(), columns=["ID", "Name", "Country", "IsIndex", "Exchange"])
        self.sectors = pd.DataFrame( self.get_sectors(), columns=["ID", "Name"])

        # Clean data
        self.instruments = self.instruments.drop_duplicates(subset=["Name"], keep="first")

        # Import stock prices
        self.stockprices = self.stockprices(self.instruments, "2003-01-01", str(datetime.today().strftime('%Y-%m-%d')))

        # Export data to CSV
        self.export_to_csv(self.markets, "Data\\markets.csv")
        self.export_to_csv(self.branches, "Data\\branches.csv")
        self.export_to_csv(self.instruments, "Data\\instruments.csv")
        self.export_to_csv(self.sectors, "Data\\sectors.csv")
        self.export_to_csv(self.stockprices, "Data\\stockprices.csv")
        print("Data from Börsdata exported successfully.")

    #----------------------------------------------------------------------------------------------------------
    def get_data(self, url):
        # Get data from HTML GET request
        try:
            r = requests.get(url = url)
            return r
        except requests.exceptions.HTTPError as errh:
            logger.error("GET Request for %s failed." % (errh))
            return None
        except requests.exceptions.ConnectionError as errc:
            logger.error("GET Request for %s failed." % (errc))
            return None
        except requests.exceptions.Timeout as errt:
            logger.error("GET Request for %s failed." % (errt))
            return None
        except requests.exceptions.RequestException as err:
            logger.error("GET Request for %s failed." % (err))
            return None

    #-----------------------------------------------------------------------------------------------------------
    def get_instruments(self):
        """ List of all instruments """

        instruments = borsdata.get_instruments()
        instrument_list = []
        temp_row = []
        # Loop through market data and create a dataset
        for i in range(0,len(instruments)):
            if instruments[i].name in PORTFOLIO_LIST:
                temp_row.append(instruments[i].insId)
                temp_row.append(instruments[i].name)
                temp_row.append(instruments[i].sectorId)
                temp_row.append(instruments[i].marketId)
                temp_row.append(instruments[i].branchId)
                temp_row.append(instruments[i].countryId)
                instrument_list.append(temp_row)
                temp_row = []
        return instrument_list

    #-----------------------------------------------------------------------------------------------------------
    def get_instruments_global(self):
        # Get settings from smart plug s
        url = "https://apiservice.borsdata.se/v1/instruments/global?authKey=%s" % API_KEY
        instruments = self.get_data(url).json()["instruments"]
        instrument_list = []
        temp_row = []
        for i in range(0,len(instruments)):
            if instruments[i]["name"] in PORTFOLIO_LIST:
                temp_row.append(instruments[i]["insId"])
                temp_row.append(instruments[i]["name"])
                temp_row.append(instruments[i]["sectorId"])
                temp_row.append(instruments[i]["marketId"])
                temp_row.append(instruments[i]["branchId"])
                temp_row.append(instruments[i]["countryId"])
                instrument_list.append(temp_row)
                temp_row = []
        return instrument_list

    #-----------------------------------------------------------------------------------------------------------
    def get_branches(self):
        branches = borsdata.get_branches()
        branch_list = []
        temp_row = []
        for i in range(0,len(branches)):
            temp_row.append(branches[i].id)
            temp_row.append(branches[i].name)
            temp_row.append(branches[i].sectorId)
            branch_list.append(temp_row)
            temp_row = []
        return branch_list

    #-----------------------------------------------------------------------------------------------------------
    def get_markets(self):
        markets = borsdata.get_markets()
        market_list = []
        temp_row = []
        # Loop through market data and create a dataset
        for i in range(0,len(markets)):
            temp_row.append(markets[i].id)
            temp_row.append(markets[i].name)
            temp_row.append(markets[i].countryId)
            temp_row.append(markets[i].isIndex)
            temp_row.append(markets[i].exchangeName)
            market_list.append(temp_row)
            temp_row = []
        return market_list

    #-----------------------------------------------------------------------------------------------------------
    def get_sectors(self):
        sectors = borsdata.get_sectors()
        sector_list = []
        temp_row = []
        for i in range(0,len(sectors)):
            temp_row.append(sectors[i].id)
            temp_row.append(sectors[i].name)
            sector_list.append(temp_row)
            temp_row = []
        return sector_list

    #-----------------------------------------------------------------------------------------------------------
    def export_to_csv(self, dataframe, filename):
        dataframe.to_csv(filename)

    #--------------------------------------------------------------------------------------------------------
    def stockprices(self, instruments, startdate, enddate):
        # List of instruments
        instrument_list = instruments["ID"].tolist()

        # Importing data
        price_list = []
        temp_row = {}
        counter = 1
        for instrument in instrument_list:
            print("Importing data for %s (%s of %s)" % (instrument, counter, len(instrument_list)))
            stock_prices = borsdata.get_instrument_stock_price(instrument, startdate, enddate)
            for i in range(0,len(stock_prices)):
                temp_row = {
                    "ID":instrument,                         
                    "Date":stock_prices[i].d,                 
                    "High":stock_prices[i].h,                 
                    "Low":stock_prices[i].l,                
                    "Close":stock_prices[i].c,             
                    "Open":stock_prices[i].o,               
                    "Volume":stock_prices[i].v
                }            
                price_list.append(temp_row)
            temp_row = {}
            counter += 1
        return pd.DataFrame(price_list, columns=("ID", "Date", "High", "Low", "Close", "Open", "Volume"))



Import()