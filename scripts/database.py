import pymongo
import pandas as pd
import numpy as np
import json

# local config file
import config

from tools import get_data_files

# params
import_file_path = config.settings["import_path"]

def importData(file_path):
    print(file_path)
    print(get_data_files(term = "US"))
    return


if __name__ == "__name__":
    importData(import_file_path)


