# Filename: merge.py
# Author: Mason Tian
# Description: Merges a list of individual stock list files and outputs
# formatted excel spreadsheets


import pandas as pd
import csv
import math

from tools import insert, get_data_files

# script params

# list of factors to call
factors = []

# MM/DD/YYYY date to start
startdate = ""

# frequency
frequency = ""
frequency = "per=" + frequency

# function to call
function = ""


if __name__ == "__main__":
    files = get_data_files("US")
    print(files)

    tickers = set()

    for f in files:
        df = pd.read_excel(open(f, 'rb'))
        tickers = set(df["Ticker"]).union(tickers)
   
    tickers = list(tickers)
    df_all = pd.DataFrame({'Ticker': tickers})
    df_all.to_excel("./divided/US_Equity.xls", header=False, \
            index=False)

    factors = []
    for factor in factors:
        flist = []
        for stock in tickers:

            function = "=" + function + "(\"" + stock + "\",\"" \
                    + factor + "\",\"" + startdate + "\",\"\",\"" \
                    + frequency + "\")"

            flist.append(function)
            flist.append(" ")
        df = pd.DataFrame({'vals':flist})
        i = 0
        while i < math.ceil(df.transpose().shape[1]/4000):
            filename = \
                    "./divided/US_Equity(" + factor + str(i) + ").csv"
            df.transpose().iloc[:,i*4000:(i+1)*4000].to_csv(filename,\
                    sep="^", header = False, index = False)
            insert(filename, "sep=^\n")
            i += 1

