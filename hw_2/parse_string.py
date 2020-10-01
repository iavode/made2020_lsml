# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 19:16:53 2020.

@author: Odegov Ilya
"""
import re

from pandas import read_csv


data = read_csv("AB_NYC_2019.csv")
prices = data.price.values
pattern = re.compile(r"[0-9]+")
prices, mean, size = [], 0, 0
with open("AB_NYC_2019.csv", "r", encoding='utf-8') as fin:
    for i, line in enumerate(fin.readlines()):
        try:
            price = int(line.strip().split(",")[-7])
            prices.append(price ** 2)
            mean += price
            size += 1
        except ValueError:
            continue
        except IndexError:
            continue

mean /= size
print(mean, sum(prices) / size - mean**2)
