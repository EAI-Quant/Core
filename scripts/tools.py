# Filename: tools.py
# Author: Mason Tian
# Description: General helper functions.

import os


# Insert a string into the beginning of originalFile
def insert(originalfile,string):
    with open(originalfile,'r') as f:
        with open('newfile.csv','w') as f2: 
            f2.write(string)
            f2.write(f.read())
    os.rename('newfile.csv',originalfile)


# Returns all file names with term in the name in the local directory
def get_data_files(term, path = "."):
    return [f for f in os.listdir(path) if term in f]

