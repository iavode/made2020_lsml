#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 31 15:12:32 2020

@author: iavode
"""
from pandas import read_csv
from sklearn.feature_extraction.text import TfidfVectorizer


def tokenizer(text: str):
    return text.lower().strip().split(" ")


reviews = read_csv("tripadvisor_hotel_reviews.csv")["Review"]  # .apply(lambda val: val)
tfidf = TfidfVectorizer(token_pattern="[A-z0-9]*")
tfifd_reviews = tfidf.fit_transform(reviews)