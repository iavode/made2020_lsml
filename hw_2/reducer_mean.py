#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 21:05:06 2020.

@author: Odegov Ilya
"""
from sys import stdin


def _compute_mean(*args):
    """Compute new mean value."""
    current_size, current_mean, new_chunk_size, new_mean = args
    mean = current_size * current_mean + new_chunk_size * new_mean
    mean /= (current_size + new_chunk_size)
    return mean


def update_static_params(*args):
    """Update static params with new data."""
    current_size, current_mean, chunk_size, new_mean = args
    current_mean = _compute_mean(*args)  # update current mean
    current_size += chunk_size  # update current data size
    return current_size, current_mean


def process_line(line: str):
    """Get line and process it."""
    line = line.rstrip("\n")  # del \n
    line = line.split("\t")[1]  # split input line and get key value
    # get size, mean
    chunk_size, mean = tuple(map(float, line.split(", ")))
    return chunk_size, mean


def get_lines(file):
    """Generator line from file."""
    for line in file:
    	yield line


def compute_static_params():
    """Compute data mean and var in mapreduce style."""
    # init values for static prams
    current_size, current_mean = 0, 0  
    lines = get_lines(stdin)	
    for line in lines:
        chunk_size, new_mean = process_line(line)
        # init params for for function update_static_params
        args = (current_size, current_mean, chunk_size, new_mean)
        # update static params by curent chunk values
        current_size, current_mean = update_static_params(*args)
    print(f"Price mean\t{round(current_mean, 12)}")


if __name__ == "__main__":
    compute_static_params()

