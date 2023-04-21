import requests
from bs4 import BeautifulSoup
import unittest
import sqlite3
import csv
import matplotlib.pyplot as plt
import numpy as np
import json
import pandas as pd
import numpy as np


#connecting database
conn = sqlite3.connect("stock.sqlite")
cur = conn.cursor()

# cur.execute('DROP TABLE IF EXISTS INTEREST_RATE')
# cur.execute('DROP TABLE IF EXISTS AAPL_PRICE')
# cur.execute('DROP TABLE IF EXISTS JPM_PRICE')
# cur.execute('DROP TABLE IF EXISTS AAPL_VOLUME')
# cur.execute('DROP TABLE IF EXISTS JPM_VOLUME')
# cur.execute('DROP TABLE IF EXISTS AAPL')
# cur.execute('DROP TABLE IF EXISTS JPM')


cur.execute("CREATE TABLE IF NOT EXISTS INTEREST_RATE (date INTEGER, avg_interest_rate FLOAT, year INTEGER)")
cur.execute("CREATE TABLE IF NOT EXISTS AAPL_PRICE (date INTEGER, stock_price FLOAT, year INTEGER)")
cur.execute("CREATE TABLE IF NOT EXISTS JPM_PRICE (date INTEGER, stock_price FLOAT, year INTEGER)")
cur.execute("CREATE TABLE IF NOT EXISTS AAPL_VOLUME (date INTEGER, stock_volume INTEGER, year INTEGER)")
cur.execute("CREATE TABLE IF NOT EXISTS JPM_VOLUME (date INTEGER, stock_volume INTEGER, year INTEGER)")


#urls
url1 = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates?filter=record_date:lt:2023-03-31"
url2 = "https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol=AAPL&apikey=GVVMWZMSSP5RKNCN"
url3 = "https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol=JPM&apikey=GVVMWZMSSP5RKNCN"



response2 = requests.get(url2)
response3 = requests.get(url3)

data2 = response2.text
data3 = response3.text

in_dict2 = json.loads(data2)
in_dict3 = json.loads(data3)

input = {}


def setUp(url):
    response = requests.get(url)
    data = response.text
    in_dict = json.loads(data)
    
    return in_dict


def load_interest_rate_data():

    year = 2001
    
    for row in cur.execute("SELECT INTEREST_RATE.year from INTEREST_RATE"):
        year = row[0] + 1
        if(row[0] == 2023):
            print("Data is only up to 2023\n")
       
    url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates?filter=record_calendar_year:eq:" + str(year)
    hash1 = setUp(url)
    
    input = {}
    for i in hash1["data"]:
        date = i["record_date"].split("-")
        year = date[0]
        month = date[1]
        date = int(date[0] + date[1])
        if date not in input:
            rate = float(i["avg_interest_rate_amt"])
            input[date] = 1
            #print(date)
            cur.execute("INSERT INTO INTEREST_RATE (date, avg_interest_rate, year) VALUES (? ,?, ?)", (date, rate, year))
            
   
    url2 =  url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates?filter=record_date:gt:" + str(year) + "-06-30"
    hash2 = setUp(url2)
    for i in hash2["data"]:
       date = i["record_date"].split("-")
       year = date[0]
       month = date[1]
       date = int(date[0] + date[1])
       if date not in input:
           rate = float(i["avg_interest_rate_amt"])
           input[date] = 1
           #print(date)
           cur.execute("INSERT INTO INTEREST_RATE (date, avg_interest_rate, year) VALUES (? ,?, ?)", (date, rate, year))
           if month == "12":
               break

            






# insert_data_year_range(2001, 2022)

# ########### FOR DEBUG AND VISUAL DELETE LATER ###############
# keys2 = in_dict2.keys() # Meta data && Montly Time Series
# key2 = in_dict2["Monthly Time Series"].keys()   # Dates Monthly from 1999-12-31 till 2023-04-14
# print(key2)

# keys3 = in_dict3.keys() # Meta data && Montly Time Series
# key3 = in_dict3["Monthly Time Series"].keys()   # Dates Monthly from 1999-12-31 till 2023-04-14
# print(key3)
# #############################################################

# for day in in_dict2["Monthly Time Series"]:
#     time = day.split("-")
#     year = time[0]
#     month = time[1]
#     date = int(year + month)
#     price = float(in_dict2["Monthly Time Series"][day]["2. high"])
#     volume = int(in_dict2["Monthly Time Series"][day]["5. volume"])
#     cur.execute("INSERT INTO AAPL_PRICE (date, stock_price, year) VALUES (?, ?, ?)", (date, price, year))
#     cur.execute("INSERT INTO AAPL_VOLUME (date, stock_volume, year) VALUES (?, ?, ?)", (date, volume, year))

# for day in in_dict3["Monthly Time Series"]:
#     time = day.split("-")
#     year = time[0]
#     month = time[1]
#     date = int(year + month)
#     price = float(in_dict3["Monthly Time Series"][day]["2. high"])
#     volume = int(in_dict3["Monthly Time Series"][day]["5. volume"])
#     cur.execute("INSERT INTO JPM_PRICE (date, stock_price, year) VALUES (?, ?, ?)", (date, price, year))
#     cur.execute("INSERT INTO JPM_VOLUME (date, stock_volume, year) VALUES (?, ?, ?)", (date, volume, year))
def insert_table_JPM():
    cur.execute('SELECT JPM_PRICE.date FROM JPM_PRICE')
    count = 0
    cur_year = 0
    for row in cur:
        cur_year = row[0]
        count += 1
    cur_year = cur_year // 100
    if cur_year == 2022:
        print("Unable to insert more data since we inserted all available data")
        return
    if count == 0:
        for day in in_dict3["Monthly Time Series"]:
            time = day.split("-")
            year = time[0]
            if int(year) != 2001:
                continue
            if int(year) < 2001:
                break
            month = time[1]
            date = int(year + month)
            price = float(in_dict3["Monthly Time Series"][day]["2. high"])
            volume = int(in_dict3["Monthly Time Series"][day]["5. volume"])
            cur.execute("INSERT INTO JPM_PRICE (date, stock_price, year) VALUES (?, ?, ?)", (date, price, year))
            cur.execute("INSERT INTO JPM_VOLUME (date, stock_volume, year) VALUES (?, ?, ?)", (date, volume, year))
    else:
        for day in in_dict3['Monthly Time Series']:
            time = day.split("-")
            year = time[0]
            if int(year) > cur_year + 1:
                continue
            if int(year) < cur_year + 1:
                break
            month = time[1]
            date = int(year + month)
            price = float(in_dict3["Monthly Time Series"][day]["2. high"])
            volume = int(in_dict3["Monthly Time Series"][day]["5. volume"])
            cur.execute("INSERT INTO JPM_PRICE (date, stock_price, year) VALUES (?, ?, ?)", (date, price, year))
            cur.execute("INSERT INTO JPM_VOLUME (date, stock_volume, year) VALUES (?, ?, ?)", (date, volume, year))
        
# def join_table(table1, table2, year):
#     cur.execute("SELECT * FROM "+ table1 + "JOIN " + table2 + "ON " + table1 + ".date = " +  table2 + ".date  WHERE " + table2 + ".year = " + str(year))

def insert_table_AAPL():
    cur.execute('SELECT AAPL_PRICE.date FROM AAPL_PRICE')
    count = 0
    cur_year = 0
    for row in cur:
        cur_year = row[0]
        count += 1
    cur_year = cur_year // 100
    if cur_year == 2022:
        print("Unable to insert more data since we inserted all available data")
        return
    if count == 0:
        for day in in_dict2['Monthly Time Series']:
            time = day.split("-")
            year = time[0]
            if int(year) != 2001:
                continue
            if int(year) < 2001:
                break
            month = time[1]
            date = int(year + month)
            price = float(in_dict2["Monthly Time Series"][day]["2. high"])
            volume = int(in_dict2["Monthly Time Series"][day]["5. volume"])
            cur.execute("INSERT INTO AAPL_PRICE (date, stock_price, year) VALUES (?, ?, ?)", (date, price, year))
            cur.execute("INSERT INTO AAPL_VOLUME (date, stock_volume, year) VALUES (?, ?, ?)", (date, volume, year))
    else:
        for day in in_dict2['Monthly Time Series']:
            time = day.split("-")
            year = time[0]
            if int(year) > cur_year + 1:
                continue
            if int(year) < cur_year + 1:
                break
            month = time[1]
            date = int(year + month)
            price = float(in_dict2["Monthly Time Series"][day]["2. high"])
            volume = int(in_dict2["Monthly Time Series"][day]["5. volume"])
            cur.execute("INSERT INTO AAPL_PRICE (date, stock_price, year) VALUES (?, ?, ?)", (date, price, year))
            cur.execute("INSERT INTO AAPL_VOLUME (date, stock_volume, year) VALUES (?, ?, ?)", (date, volume, year))



def AAPL_PRICE_TIMELINE():
    date = []
    price = [] 
    for row in cur.execute('SELECT AAPL_PRICE.date, AAPL_PRICE.stock_price FROM AAPL_PRICE' ):
        date.append(row[0])
        price.append(row[1])
    plt.plot(date, price)

    font1 = {'family':'serif','color':'blue','size':20}
    font2 = {'family':'serif','color':'darkred','size':15}
    plt.title("AAPL Monthly Price Variation", fontdict = font1)
    plt.xlabel("Time (YEAR MONTH)", fontdict = font2)
    plt.ylabel("AAPL Stock Price ($)", fontdict = font2)
    plt.show()

def AAPL_VOLUME_TIMELINE():
    date = []
    volume = [] 
    for row in cur.execute('SELECT AAPL_VOLUME.date, AAPL_VOLUME.stock_volume FROM AAPL_VOLUME' ):
        date.append(row[0])
        volume.append(row[1])
    plt.plot(date, volume)

    font1 = {'family':'serif','color':'blue','size':20}
    font2 = {'family':'serif','color':'darkred','size':15}
    plt.title("AAPL Monthly Volume Variation", fontdict = font1)
    plt.xlabel("Time (YEAR MONTH)", fontdict = font2)
    plt.ylabel("AAPL Stock Volume", fontdict = font2)
    plt.show()


def JPM_PRICE_TIMELINE():
    date = []
    price = [] 
    for row in cur.execute('SELECT JPM_PRICE.date, JPM_PRICE.stock_price FROM JPM_PRICE' ):
        date.append(row[0])
        price.append(row[1])
    plt.plot(date, price)

    font1 = {'family':'serif','color':'blue','size':20}
    font2 = {'family':'serif','color':'darkred','size':15}
    plt.title("JPM Monthly Price Variation", fontdict = font1)
    plt.xlabel("Time (YEAR MONTH)", fontdict = font2)
    plt.ylabel("JPM Stock Price ($)", fontdict = font2)
    plt.show()
    

def JPM_VOLUME_TIMELINE():
    date = []
    volume = [] 
    for row in cur.execute('SELECT JPM_VOLUME.date, JPM_VOLUME.stock_volume FROM JPM_VOLUME' ):
        date.append(row[0])
        volume.append(row[1])
    plt.plot(date, volume)

    font1 = {'family':'serif','color':'blue','size':20}
    font2 = {'family':'serif','color':'darkred','size':15}
    plt.title("JPM Monthly Volume Variation", fontdict = font1)
    plt.xlabel("Time (YEAR MONTH)", fontdict = font2)
    plt.ylabel("JPM Stock Volume", fontdict = font2)
    plt.show()


def join_interest_rate_apple_price():
    cur.execute("SELECT AAPL_PRICE.date, INTEREST_RATE.avg_interest_rate, AAPL_PRICE.stock_price FROM AAPL_PRICE JOIN INTEREST_RATE ON AAPL_PRICE.date = INTEREST_RATE.date WHERE INTEREST_RATE.year >= 2001")

def join_interest_rate_apple_volume():
    cur.execute("SELECT * FROM AAPL_VOLUME JOIN INTEREST_RATE ON AAPL_VOLUME.date = INTEREST_RATE.date WHERE INTEREST_RATE.year >= 2001")

def join_interest_rate_jpm_price():
    cur.execute("SELECT JPM_PRICE.date, INTEREST_RATE.avg_interest_rate, JPM_PRICE.stock_price FROM JPM_PRICE JOIN INTEREST_RATE ON JPM_PRICE.date = INTEREST_RATE.date WHERE INTEREST_RATE.year >= 2001")

def join_interest_rate_jpm_volume():
    cur.execute("SELECT * FROM JPM_VOLUME JOIN INTEREST_RATE ON JPM_VOLUME.date = INTEREST_RATE.date WHERE INTEREST_RATE.year >= 2001")


def AAPL_PRICE_Interest_rate_correlation():

    join_interest_rate_apple_price()
    lst = []
    lst2 = []
    for row in cur:
        price = row[1]
        rate = row[4]
        lst.append(price)
        lst2.append(rate)
    
    x = pd.Series(lst)
    y = pd.Series(lst2)
    correlation = y.corr(x)
    plt.scatter(x, y)

    plt.plot(np.unique(x), np.poly1d(np.polyfit(x, y, 1)) (np.unique(x)), color='red')

    font1 = {'family':'serif','color':'blue','size':10}
    font2 = {'family':'serif','color':'darkred','size':15}
    plt.title("Correlation between AAPL_PRICE and INTEREST_RATE", fontdict = font1)
    plt.xlabel("AAPL_PRICE", fontdict = font2)
    plt.ylabel("INTEREST RATE", fontdict = font2)
    plt.show()

def AAPL_VOLUME_Interest_rate_correlation():

    join_interest_rate_apple_volume()
    lst = []
    lst2 = []
    for row in cur:
        price = row[1]
        rate = row[4]
        lst.append(price)
        lst2.append(rate)
    
    x = pd.Series(lst)
    y = pd.Series(lst2)
    correlation = y.corr(x)
    plt.scatter(x, y)

    plt.plot(np.unique(x), np.poly1d(np.polyfit(x, y, 1)) (np.unique(x)), color='red')

    font1 = {'family':'serif','color':'blue','size':10}
    font2 = {'family':'serif','color':'darkred','size':9}
    plt.title("Correlation between AAPL_VOLUME and INTEREST_RATE", fontdict = font1)
    plt.xlabel("Percentage change of AAPL_VOLUME", fontdict = font2)
    plt.ylabel("Percentage change of INTEREST RATE", fontdict = font2)
    plt.show()

def change_AAPL_PRICE_Interest_rate_correlation(lst, lst2):

    x = pd.Series(lst2)
    y = pd.Series(lst)
    correlation = y.corr(x)
    plt.scatter(x, y)

    plt.plot(np.unique(x), np.poly1d(np.polyfit(x, y, 1)) (np.unique(x)), color='red')

    font1 = {'family':'serif','color':'blue','size':10}
    font2 = {'family':'serif','color':'darkred','size':9}
    plt.title("Correlation between AAPL_PRICE and INTEREST_RATE", fontdict = font1)
    plt.xlabel("Percentage change of INTEREST RATE", fontdict = font2)
    plt.ylabel("Percentage change of AAPL_PRICE", fontdict = font2)
    plt.show()

def change_JPM_PRICE_Interest_rate_correlation(lst, lst2):

    x = pd.Series(lst2)
    y = pd.Series(lst)
    correlation = y.corr(x)
    plt.scatter(x, y)

    plt.plot(np.unique(x), np.poly1d(np.polyfit(x, y, 1)) (np.unique(x)), color='red')

    font1 = {'family':'serif','color':'blue','size':10}
    font2 = {'family':'serif','color':'darkred','size':9}
    plt.title("Correlation between JPM_PRICE and INTEREST_RATE", fontdict = font1)
    plt.xlabel("Percentage change of INTEREST RATE", fontdict = font2)
    plt.ylabel("Percentage change of JPM_PRICE", fontdict = font2)
    plt.show()


def change_AAPL_VOLUME_Interest_rate_correlation(lst, lst2):

    x = pd.Series(lst2)
    y = pd.Series(lst)
    correlation = y.corr(x)
    plt.scatter(x, y)

    plt.plot(np.unique(x), np.poly1d(np.polyfit(x, y, 1)) (np.unique(x)), color='red')

    font1 = {'family':'serif','color':'blue','size':10}
    font2 = {'family':'serif','color':'darkred','size':9}
    plt.title("Correlation between Percentage Change Of AAPL_VOLUME and INTEREST_RATE", fontdict = font1)
    plt.xlabel("Percentage change of INTEREST RATE", fontdict = font2)
    plt.ylabel("Percentage change of AAPL_VOLUME", fontdict = font2)
    plt.show()

def change_JPM_VOLUME_Interest_rate_correlation(lst, lst2):

    x = pd.Series(lst2)
    y = pd.Series(lst)
    correlation = y.corr(x)
    plt.scatter(x, y)

    plt.plot(np.unique(x), np.poly1d(np.polyfit(x, y, 1)) (np.unique(x)), color='red')

    font1 = {'family':'serif','color':'blue','size':10}
    font2 = {'family':'serif','color':'darkred','size':9}
    plt.title("Correlation between Percentage Change Of JPM_VOLUME and INTEREST_RATE", fontdict = font1)
    plt.xlabel("Percentage change of INTEREST RATE", fontdict = font2)
    plt.ylabel("Percentage change of JPM_VOLUME", fontdict = font2)
    plt.show()

def write_AAPL_data_csv():
    outFile = open("aapl_data.csv", "w")
    csvOut = csv.writer(outFile)

    cur.execute("SELECT AAPL_VOLUME.stock_volume FROM AAPL_VOLUME")

    volumelst = []
    for i in cur:
        volumelst.append(i[0])
        

    join_interest_rate_apple_price()
    header = ("Date", "avg_interest_rate", "stock_price", "stock_volume", "Percent change of price", "Percent change of interest rate", "Percent change of Volume")
    csvOut.writerow(header)

    changeRate = 0
    prevPrice = 0
    prevRate = 0
    prevDate = 0
    prevVolume = 0
    
    volchangelst = []
    pricechangelst = []
    ratechangelst = []
    index = 0
    for row in cur:

        if row[0] == prevDate:
            continue

        if(row[0] != 200101):
            changeRate = (prevRate + row[1]) / prevRate
            changeRate = round(changeRate , 2)
            
            if(prevRate - row[1] < 0):
                changeRatestr = "+" + str(changeRate) + "%"
            elif(prevRate - row[1] > 0):
                changeRatestr = "-" + str(changeRate) + "%"
            else:
                changeRatestr = str(changeRate) + "%"

            changePrice = (prevPrice + row[2]) / prevPrice
            changePrice = round(changePrice, 2)
            

            if(prevPrice - row[2] < 0):
                changePricestr = "+" + str(changePrice) + "%"
            elif(prevPrice - row[2] > 0):
                changePricestr = "-" + str(changePrice) + "%"
            else:
                changePricestr = str(changePrice) + "%"


            changeVolume = (prevVolume + volumelst[index]) / prevVolume
            changeVolume = round(changeVolume, 2)
            

            if(prevVolume - volumelst[index] < 0):
                changeVolumestr = "+" + str(changeVolume) + "%"
            elif(prevVolume - volumelst[index] > 0):
                changeVolumestr = "-" + str(changeVolume) + "%"
            else:
                changeVolumestr = str(changeVolume) + "%"

            
        else:
            #vpt = 0
            changeRate = 0
            changePrice = 0
            changeVolume = 0

            changeRatestr = "0"
            changePricestr = "0"
            changeVolumestr = "0"


        volchangelst.append(changeVolume)
        pricechangelst.append(changePrice)
        ratechangelst.append(changeRate)
        

        prevVolume = volumelst[index]
        prevPrice = row[2]
        prevRate = row[1]
        prevDate = row[0]
        content = (row[0], row[1], row[2], volumelst[index], changePricestr, changeRatestr, changeVolumestr)
        index += 1
        csvOut.writerow(content)
    
    pricechangelst.pop(0)
    ratechangelst.pop(0)
    volchangelst.pop(0)

    change_AAPL_VOLUME_Interest_rate_correlation(volchangelst, ratechangelst)
    #change_AAPL_PRICE_Interest_rate_correlation(pricechangelst, ratechangelst)

def write_JPM_data_csv():
    outFile = open("JPM_data.csv", "w")
    csvOut = csv.writer(outFile)

    cur.execute("SELECT JPM_VOLUME.stock_volume FROM JPM_VOLUME")

    volumelst = []
    for i in cur:
        volumelst.append(i[0])
        

    join_interest_rate_jpm_price()
    header = ("Date", "avg_interest_rate", "stock_price", "stock_volume", "Percent change of price", "Percent change of interest rate", "Percent change of Volume")
    csvOut.writerow(header)

    changeRate = 0
    prevPrice = 0
    prevRate = 0
    prevDate = 0
    prevVolume = 0
    
    volchangelst = []
    pricechangelst = []
    ratechangelst = []
    index = 0
    for row in cur:

        if row[0] == prevDate:
            continue

        if(row[0] != 200101):
            changeRate = (prevRate + row[1]) / prevRate
            changeRate = round(changeRate , 2)
            
            if(prevRate - row[1] < 0):
                changeRatestr = "+" + str(changeRate) + "%"
            elif(prevRate - row[1] > 0):
                changeRatestr = "-" + str(changeRate) + "%"
            else:
                changeRatestr = str(changeRate) + "%"

            changePrice = (prevPrice + row[2]) / prevPrice
            changePrice = round(changePrice, 2)
            

            if(prevPrice - row[2] < 0):
                changePricestr = "+" + str(changePrice) + "%"
            elif(prevPrice - row[2] > 0):
                changePricestr = "-" + str(changePrice) + "%"
            else:
                changePricestr = str(changePrice) + "%"


            changeVolume = (prevVolume + volumelst[index]) / prevVolume
            changeVolume = round(changeVolume, 2)
            

            if(prevVolume - volumelst[index] < 0):
                changeVolumestr = "+" + str(changeVolume) + "%"
            elif(prevVolume - volumelst[index] > 0):
                changeVolumestr = "-" + str(changeVolume) + "%"
            else:
                changeVolumestr = str(changeVolume) + "%"

            
        else:
            #vpt = 0
            changeRate = 0
            changePrice = 0
            changeVolume = 0

            changeRatestr = "0"
            changePricestr = "0"
            changeVolumestr = "0"


        volchangelst.append(changeVolume)
        pricechangelst.append(changePrice)
        ratechangelst.append(changeRate)
        

        prevVolume = volumelst[index]
        prevPrice = row[2]
        prevRate = row[1]
        prevDate = row[0]
        content = (row[0], row[1], row[2], volumelst[index], changePricestr, changeRatestr, changeVolumestr)
        index += 1
        csvOut.writerow(content)
    
    pricechangelst.pop(0)
    ratechangelst.pop(0)
    volchangelst.pop(0)

    #change_JPM_VOLUME_Interest_rate_correlation(volchangelst, ratechangelst)
    #change_JPM_PRICE_Interest_rate_correlation(pricechangelst, ratechangelst)





# load_interest_rate_data()
# insert_table_AAPL()
# insert_table_JPM()

# write_AAPL_data_csv()
# write_JPM_data_csv()

#JPM_VOLUME_TIMELINE()
#AAPL_VOLUME_TIMELINE()
#JPM_PRICE_TIMELINE()
#AAPL_PRICE_TIMELINE()

#change_JPM_VOLUME_Interest_rate_correlation(volchangelst, ratechangelst)
#change_JPM_PRICE_Interest_rate_correlation(pricechangelst, ratechangelst)
#change_AAPL_VOLUME_Interest_rate_correlation(volchangelst, ratechangelst)
#change_AAPL_PRICE_Interest_rate_correlation(pricechangelst, ratechangelst)

#join_interest_rate_jpm_price()
#join_interest_rate_apple_price()


conn.commit()
cur.close()