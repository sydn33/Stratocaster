from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import ListOfHistoricalTickLast # need this import for midpoint
from ibapi.common import BarData

import threading
import time
import datetime
import csv
from csv import writer
import pandas as pd


# choose list of dates to download 1s data (w/ volume)
def generate_bizz_days_in_date_range(start_time, end_time): # US date format eg) '12/24/2022'
    thingo = pd.bdate_range(start = start_time, end = end_time)
    timestrings = []
    for i in range(0, len(thingo)):
        time = thingo[i] 
        t = time.to_pydatetime()
        day = t.day 
        month = t.month
        year = t.year
        if day < 10 and month < 10:
            timestrings.append( str(year) + '0' + str(month) + '0' + str(day))
        elif day < 10 and month >= 10:
            timestrings.append( str(year) + str(month) + '0' + str(day))
        elif day >= 10 and month < 10:
            timestrings.append( str(year) + '0' + str(month) + str(day))
        else:
            timestrings.append( str(year) + str(month) + str(day))
    return timestrings

bizz_dates = generate_bizz_days_in_date_range(start_time = '3/1/2023', end_time = '6/15/2023') # US date format eg) '12/24/2022'
print(f'Business dates in range: {bizz_dates}')
time.sleep(5)

for date in bizz_dates:
    # create contract object
    symbol_to_write = 'NQM3'
    fut_contract = Contract()
    fut_contract.symbol = "NQ" # use just NQ if expired, otherwise the actual live ticker eg) NQZ2 before Dec expiry
    fut_contract.secType = "FUT"
    fut_contract.currency = "USD"
    fut_contract.exchange = "CME"
    fut_contract.lastTradeDateOrContractMonth = '202306'
    fut_contract.includeExpired = True 

    # create empty csv file for chosen date
    dataframe = pd.DataFrame(list())
    # writing empty DataFrame to the new csv file
    dataframe.to_csv(f'/Users/scottweatherill/Desktop/blindsight/futures_data/{symbol_to_write}-{date}.csv')

    class IBapi(EWrapper, EClient):
        def __init__(self):
            EClient.__init__(self, self)

        def tickPrice(self, reqId, tickType, price, attrib):
            if tickType == 2 and reqId == 1:
                print('The current ask price is: ', price)
        
        def historicalTicksLast(self, reqId: int, ticks: ListOfHistoricalTickLast, done: bool):
            for tick in ticks:
                print("HistoricalTickLast. ReqId:", reqId, tick)
        
        def historicalData(self, reqId:int, bar: BarData):
            #print("HistoricalData. ReqId:", reqId, "BarData.", bar)
            line_to_add = [bar.date, symbol_to_write, bar.open, bar.high, bar.low, bar.close, bar.volume]
            print(line_to_add)
            with open(f'/Users/scottweatherill/Desktop/blindsight/futures_data/{symbol_to_write}-{date}.csv', 'a') as f_object:
                writer_object = writer(f_object)
                writer_object.writerow(line_to_add)

    def run_loop():
        app.run()

    time.sleep(2)
    app = IBapi()
    app.connect('127.0.0.1', 7496, 0)
    print("API connection established")

    #Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    time.sleep(2) #Sleep interval to allow time for connection to server

    #######################################################

    endQueryTime1 = date + '-' + '00:29:59'
    endQueryTime2 = date + '-' + '00:59:59'
    endQueryTime3 = date + '-' + '01:29:59'
    endQueryTime4 = date + '-' + '01:59:59'
    endQueryTime5 = date + '-' + '02:29:59'
    endQueryTime6 = date + '-' + '02:59:59'
    endQueryTime7 = date + '-' + '03:29:59'
    endQueryTime8 = date + '-' + '03:59:59'
    endQueryTime9 = date + '-' + '04:29:59'
    endQueryTime10 = date + '-' + '04:59:59'
    endQueryTime11 = date + '-' + '05:29:59'
    endQueryTime12 = date + '-' + '05:59:59'
    endQueryTime13 = date + '-' + '06:29:59'
    endQueryTime14 = date + '-' + '06:59:59'
    endQueryTime15 = date + '-' + '07:29:59'
    endQueryTime16 = date + '-' + '07:59:59'
    endQueryTime17 = date + '-' + '08:29:59'
    endQueryTime18 = date + '-' + '08:59:59'
    endQueryTime19 = date + '-' + '09:29:59'
    endQueryTime20 = date + '-' + '09:59:59'
    endQueryTime21 = date + '-' + '10:29:59'
    endQueryTime22 = date + '-' + '10:59:59'
    endQueryTime23 = date + '-' + '11:29:59'
    endQueryTime24 = date + '-' + '11:59:59'
    endQueryTime25 = date + '-' + '12:29:59'
    endQueryTime26 = date + '-' + '12:59:59'
    endQueryTime27 = date + '-' + '13:29:59'
    endQueryTime28 = date + '-' + '13:59:59'
    endQueryTime29 = date + '-' + '14:29:59'
    endQueryTime30 = date + '-' + '14:59:59'
    endQueryTime31 = date + '-' + '15:29:59'
    endQueryTime32 = date + '-' + '15:59:59'
    endQueryTime33 = date + '-' + '16:29:59'
    endQueryTime34 = date + '-' + '16:59:59'
    endQueryTime35 = date + '-' + '17:29:59'
    endQueryTime36 = date + '-' + '17:59:59'
    endQueryTime37 = date + '-' + '18:29:59'
    endQueryTime38 = date + '-' + '18:59:59'
    endQueryTime39 = date + '-' + '19:29:59'
    endQueryTime40 = date + '-' + '19:59:59'
    endQueryTime41 = date + '-' + '20:29:59'
    endQueryTime42 = date + '-' + '20:59:59'
    endQueryTime43 = date + '-' + '21:29:59'
    endQueryTime44 = date + '-' + '21:59:59'
    endQueryTime45 = date + '-' + '22:29:59'
    endQueryTime46 = date + '-' + '22:59:59'
    endQueryTime47 = date + '-' + '23:29:59'
    endQueryTime48 = date + '-' + '23:59:59'

    app.reqHistoricalData(1001, fut_contract, endQueryTime1, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1002, fut_contract, endQueryTime2, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1003, fut_contract, endQueryTime3, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1004, fut_contract, endQueryTime4, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1005, fut_contract, endQueryTime5, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1006, fut_contract, endQueryTime6, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1007, fut_contract, endQueryTime7, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1008, fut_contract, endQueryTime8, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1009, fut_contract, endQueryTime9, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1010, fut_contract, endQueryTime10, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1011, fut_contract, endQueryTime11, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1012, fut_contract, endQueryTime12, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1013, fut_contract, endQueryTime13, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1014, fut_contract, endQueryTime14, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1015, fut_contract, endQueryTime15, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1016, fut_contract, endQueryTime16, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1017, fut_contract, endQueryTime17, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1018, fut_contract, endQueryTime18, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1019, fut_contract, endQueryTime19, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1020, fut_contract, endQueryTime20, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1021, fut_contract, endQueryTime21, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1022, fut_contract, endQueryTime22, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1023, fut_contract, endQueryTime23, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1024, fut_contract, endQueryTime24, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1025, fut_contract, endQueryTime25, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1026, fut_contract, endQueryTime26, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1027, fut_contract, endQueryTime27, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1028, fut_contract, endQueryTime28, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1029, fut_contract, endQueryTime29, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1030, fut_contract, endQueryTime30, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1031, fut_contract, endQueryTime31, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1032, fut_contract, endQueryTime32, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1033, fut_contract, endQueryTime33, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1034, fut_contract, endQueryTime34, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1035, fut_contract, endQueryTime35, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1036, fut_contract, endQueryTime36, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1037, fut_contract, endQueryTime37, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1038, fut_contract, endQueryTime38, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1039, fut_contract, endQueryTime39, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1040, fut_contract, endQueryTime40, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1041, fut_contract, endQueryTime41, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1042, fut_contract, endQueryTime42, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1043, fut_contract, endQueryTime43, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1044, fut_contract, endQueryTime44, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1045, fut_contract, endQueryTime45, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1046, fut_contract, endQueryTime46, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1047, fut_contract, endQueryTime47, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(10)
    app.reqHistoricalData(1048, fut_contract, endQueryTime48, "1800 S", "1 secs", "TRADES", 0, 1, False, [])
    time.sleep(5)

    print('wait!...')
    time.sleep(115)
    print('good to reload')
    app.disconnect()
    print("API disconnected")



       