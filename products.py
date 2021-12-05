import pandas as pd
import json
import sys


def get_top_items():
    """Returns items with 500 or more likes."""
    JSON_FILES = 286 # files start with 0
    JSON_PATH = "./data_input/listings/{i:04}.json"
    LIKES = 500

    top_items = pd.DataFrame()
    for i in range(JSON_FILES):
        df = pd.read_json(JSON_PATH.format(i=i))
        df = df[df.num_favorers >= LIKES]
        top_items = pd.concat([top_items, df])
        print("working on file {i} of {n}".format(i=i, n=JSON_FILES))
    return top_items


def save_top_items(df):
    """Saves top items df."""
    SAVE_PATH = "./data_intermediate/top_items.pkl"
    df.to_pickle(SAVE_PATH)


def main():
    """Prepares seeder data from Etsy dataset."""
    top_items = get_top_items()
    save_top_items(top_items)


if __name__ == "__main__":
    main()
