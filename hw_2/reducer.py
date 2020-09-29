#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 21:05:06 2020.

@author: Odegov Ilya
"""
from sys import stdin


def _compute_var(*args: list):
    """Compute new var value."""
    size, current_mean, current_var, chunk_size, new_mean, new_var = args
    var = size * current_var + chunk_size * new_var
    new_size = size + chunk_size
    var /= new_size
    var += chunk_size * size * ((current_mean - new_mean) / new_size) ** 2
    return var


def _compute_mean(*args: list):
    """Compute new mean value."""
    chunk_size, current_mean, new_chunk_size, new_mean = args
    mean = chunk_size * current_mean + new_chunk_size * new_mean
    mean /= (chunk_size + new_chunk_size)
    return mean


def update_stats(*args: list):
    """Update stats params with new."""
    size, current_mean, current_var, chunk_size, new_mean, _ = args
    # init params for for function _compute_var(args)
    args_for_mean = [size, current_mean, chunk_size, new_mean]
    # params for _compute_var() equal input params
    current_mean = _compute_mean(args_for_mean)  # update mean
    current_var = _compute_var(args)  # update var
    size += chunk_size  # update data size
    return size, current_mean, current_var


def process_line(line: str):
    """Get line and process it."""
    line = line.rstrip("\n")  # del \n
    line = line.split(" ")  # split input line
    chunk_size, mean, var = tuple(map(int, line))
    return chunk_size, mean, var


def compute_static_params():
    """
    Compute mean and val for all dataset.

    Compute mean and var in mapreduce style.
    """
    size, current_mean, current_var = 0, 0, 0  # init values for stats prams
    for line in stdin:
        chunk_size, new_mean, new_var = process_line(line)
        # init params for for function compute_var(args)
        args = [size, current_mean, current_var, chunk_size, new_mean, new_var]
        size, current_mean, current_var = update_stats(args)
    print(f"Price mean {current_mean} with var {current_var}")


if __name__ == "__main__":
    compute_static_params()
