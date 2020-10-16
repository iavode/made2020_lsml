#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 21:15:03 2020.

@author: Odegov Ilya
"""
from sys import stdin


def process_line(line: str) -> int:
    """Process input line."""
    # check input line type
    price = int(line.strip().split(",")[-7])
    return price


def get_line(lines):
    """Generator for input line."""
    for line in lines:
        yield line


def process_data():
    """Process input data."""
    prices_sum, size = 0, 0
    lines = get_line(stdin)
    for line in lines:
        try:
            price = process_line(line)
        except ValueError:
            continue
        except IndexError:
            continue
        prices_sum += price
        size += 1
    # compute chunk size, mean
    mean = prices_sum / size
    print(f"chunk\t{size}, {mean}")


if __name__ == "__main__":
    process_data()
