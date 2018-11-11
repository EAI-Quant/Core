# Filename: tools.py
# Author: Mason Tian
# Description: Database functions.


import pymongo
import pandas as pd
import numpy as np
import json
import math

# local config file
import config

from tools import get_data_files, return_numeric, repeat_elements

# params

# from config file
import_file_path = config.settings["data_import_path"]
list_path = config.settings["list_import_path"]

# list of identifying roots within the file names
identifiers = []


def import_data(list_path, data_files_path, identifiers):

    client = get_client()
    db = client["Stocks"]

    names = list_path + "/" + get_data_files(term = "US",\
            path = list_path)[0]
    df_names = pd.read_excel(open(names, 'rb'), header = None)
    stocks = df_names.values.T.tolist()[0]
    tail_file_length = len(df_names) % 2000

    for identifier in identifiers:
        path = data_files_path + "/"
        files = sorted(get_data_files(term = identifier,\
            path = data_files_path), key = return_numeric)


        for j, f in enumerate(files):
            if not f == files[-1]:
                df = pd.read_csv(open(path + f, 'rb'), header=None, \
                        low_memory = False, \
                        parse_dates = [i * 2 for i in range(2000)])
            else:
                df = pd.read_csv(open(path + f, 'rb'), header=None, \
                        low_memory = False, \
                        parse_dates = \
                        [i * 2 for i in range(tail_file_length)])

            for i, stock in enumerate(stocks[2000*j: \
                    min(2000 * (j+1), len(stocks))]):
                vals = list(filter(lambda x: \
                        not (pd.isnull(x[0]) or pd.isnull(x[1])), \
                        zip(df[2*i], df[2*i+1])))

                vals = list(map(lambda x: \
                        {"Date": x[0].to_pydatetime(), \
                        identifier: x[1]}, vals))

                for doc in vals:
                    old_doc = db[stock].find_one({"Date": \
                            doc["Date"]})
                    if old_doc is None:
                        old_doc = doc
                    else:
                        for field in doc:
                            old_doc[field] = doc[field]
                    db[stock].update_one({'Date':doc["Date"]}, \
                            {"$set": old_doc}, upsert=True)

    return

def get_client():
    return pymongo.MongoClient('localhost', 27017, maxPoolSize=100)

def get_valid_db(client):
    return client.list_database_names()


if __name__ == "__main__":
    import_data(list_path, import_file_path, identifiers)


