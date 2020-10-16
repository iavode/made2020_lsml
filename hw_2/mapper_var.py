#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 21:15:03 2020.

@author: Odegov Ilya
"""
from sys import stdin


def process_line(line) -> int:
    """Process input line."""
    # check input line type
    price = int(line.strip().split(",")[-7])
    return price


def calculate_static_params(size: int, prices_sum: int, prices_squad_sum: int):
    """Calculate chunk mean, var"""
    if not size:
        return 0, 0
    mean = prices_sum / size
    var = prices_squad_sum / size - mean**2
    return mean, var


def get_line(lines):
    """Generate for input line."""
    for line in lines:
        yield line


def process_data():
    """Process input data."""
    size, prices_sum, prices_squad_sum = 0, 0, 0
    lines = get_line(stdin)
    for line in lines:
        try:
            price = process_line(line)
            prices_sum += price
        except ValueError:
            continue
        except IndexError:
            continue
        prices_squad_sum += price**2
        size += 1
    # compute chunk size, mean, var
    mean, var = calculate_static_params(size, prices_sum, prices_squad_sum)
    print(f"chunk\t{size}, {mean}, {var}")


if __name__ == "__main__":
    process_data()
