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


def import_data(client, list_path, data_files_path, identifiers):

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

def delete_same_company(client):
    db = client["Stocks"]
    l = db.list_collection_names()
    i = list(filter(lambda x: x is not -1, map(lambda x: x.find('/'), l)))
    lm = list(filter(lambda x: '/' in x, l))
    t = zip(lm, i)
    results = list(map(lambda x: x[0][:x[1]], t))
    print(results)
    cannotremove = list(filter(lambda x: x[0][:x[1]] not in l, t))
    print(cannotremove)

    print(lm)

    if len(cannotremove) is 0:
        for name in lm:
            db[name].drop()
    return


if __name__ == "__main__":
    client = get_client()
    # import_data(client, list_path, import_file_path, identifiers)
    delete_same_company(client)


