import pandas as pd
import json
import sys
import os


JSON_FILES = 286 # files start with 0
JSON_PATH = "./data_input/listings/{i:04}.json"
LIKES = 500
SAVE_PATH_TOP_ITEMS = "./data_intermediate/top_items.pkl"
SAVE_PATH_FINAL_ITEMS = "./data_final/top_items.json"
KEEP_ITEMS = 3000

def get_top_items():
    """Returns items with 500 or more likes."""
    top_items = pd.DataFrame()
    for i in range(JSON_FILES):
        df = pd.read_json(JSON_PATH.format(i=i))
        df = df[df.num_favorers >= LIKES]
        top_items = pd.concat([top_items, df])
        print("working on file {i} of {n}".format(i=i, n=JSON_FILES))
    return top_items


def save_top_items(df):
    """Saves unfiltered top items df."""
    df.to_pickle(SAVE_PATH_TOP_ITEMS)


def extract_fields(df):
    """Extracts relevant data from record."""
    df["images"] = df["Images"].apply(get_image_links)
    df["category_path_ids"] = df["category_path_ids"].apply(lambda x: x[0:3])
    df["category_id"] = df["category_path_ids"].apply(lambda x: x[-1])
    df["handmade"] = df["who_made"].apply(lambda x: x == "i_did")
    keep = [
        "title", "description", "price", "category_path_ids", "category_id",
        "materials", "images", "handmade"
    ]
    return df.loc[:, keep]

def get_image_links(images):
    """Get image links from image data."""
    img_keys = ["url_75x75", "url_170x135", "url_570xN", "url_fullxfull"]
    return [dict([(k, v) for (k, v) in img.items() if k in img_keys]) for img in images]


def main():
    """Prepares seeder data from Etsy dataset."""
    if os.path.exists(SAVE_PATH_TOP_ITEMS):
        print("Loading pre-compiled df of top items (500+ likes)")
        top_items = pd.read_pickle(SAVE_PATH_TOP_ITEMS)
    else:
        print("Getting items with 500+ likes from each json file.")
        top_items = get_top_items()
        print("Saving to {fp}".format(fp=SAVE_PATH_TOP_ITEMS))
        save_top_items(top_items)
    relevant_data = extract_fields(top_items)
    relevant_data.sample(n=KEEP_ITEMS).to_json(SAVE_PATH_FINAL_ITEMS, orient="records")


if __name__ == "__main__":
    main()
