#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 10 10:08:35 2025

@author: algo-env
"""
    
import csv

url = '/home/algo-env/Downloads/award_numbers3.csv' # already active
write_url = '/home/algo-env/Downloads/award_numbers4.csv' # new file to scrape
last_award = ''

def read_csv_to_array(filename):
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        return list(reader)
        
array,index = read_csv_to_array(url),0

for i in range(len(array)):
    if array[i][0] == last_award:
        index = i
        print(i, 'finished out of: ', len(array))

def write_list_to_csv(filename, data):
    with open(filename, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(data)

# write_url.csv has been updated to include only unfinished awards
write_list_to_csv(write_url, array[index:])



