#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 21:05:06 2020.

@author: Odegov Ilya
"""
from sys import stdin


def _compute_var(*args):
    """Compute new var value."""
    size, current_mean, current_var, chunk_size, new_mean, new_var = args
    var = size * current_var + chunk_size * new_var
    new_size = size + chunk_size
    var /= new_size
    var += chunk_size * size * ((current_mean - new_mean) / new_size) ** 2
    return var


def _compute_mean(*args):
    """Compute new mean value."""
    current_size, current_mean, new_chunk_size, new_mean = args
    mean = current_size * current_mean + new_chunk_size * new_mean
    mean /= (current_size + new_chunk_size)
    return mean


def update_static_params(*args):
    """Update static params with new data."""
    current_size, current_mean, current_var, chunk_size, new_mean, _ = args
    # init params for for function _compute_var(args)
    args_for_mean = [current_size, current_mean, chunk_size, new_mean]
    # params for _compute_var() equal input params
    current_mean = _compute_mean(*args_for_mean)  # update current mean
    current_var = _compute_var(*args)  # update current var
    current_size += chunk_size  # update current data size
    return current_size, current_mean, current_var


def process_line(line: str):
    """Get line and process it."""
    print(line)
    line = line.rstrip("\n")  # del \n
    line = line.split("\t")[0][1:-1]  # split input line and get key value
    # get size, mean var
    chunk_size, mean, var = tuple(map(float, line.split(", ")))
    return chunk_size, mean, var


def get_lines(file):
    """Generator line from file."""
    for line in file:
    	yield line


def compute_static_params():
    """
    Compute data mean and var in mapreduce style.
    """
    size, current_mean, current_var = 0, 0, 0  # init values for static prams
    lines = get_lines(stdin)	
    for line in lines:
        chunk_size, new_mean, new_var = process_line(line)
        # init params for for function update_static_params
        args = [size, current_mean, current_var, chunk_size, new_mean, new_var]
        # update static params by curent chunk values
        size, current_mean, current_var = update_static_params(*args)
    print(f"Price mean\t{current_mean}")
    print(f"Price var\t{current_var}")


if __name__ == "__main__":
    compute_static_params()
