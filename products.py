import pandas as pd
import json
import sys
import os


JSON_FILES = 286 # files start with 0
JSON_PATH = "./data_input/listings/{i:04}.json"
LIKES = 500
BAN_WORDS= "./.ban_strings"
SAVE_PATH_TOP_ITEMS = "./data_intermediate/top_items.pkl"
SAVE_PATH_FINAL_ITEMS = "./data_final/top_items.json"
SAVE_PATH_FINAL_CATEGORIES = "./data_final/top_items_categories.json"
KEEP_ITEMS = 3000
CATEGORIES_FP = "./data_input/categories.json"


def get_top_items():
    """Returns items with 500 or more likes."""
    top_items = pd.DataFrame()
    ban_list = load_ban_list()
    for i in range(JSON_FILES):
        df = pd.read_json(JSON_PATH.format(i=i))
        df = df[df.num_favorers >= LIKES]
        top_items = pd.concat([top_items, df])
        print("working on file {i} of {n}".format(i=i, n=JSON_FILES))
    return top_items


def save_top_items(df):
    """Saves unfiltered top items df."""
    df.to_pickle(SAVE_PATH_TOP_ITEMS)


def load_ban_list():
    try:
        list = []
        with open(BAN_WORDS) as file:
            for line in file:
                list.append(line.strip())
        print("Ban list {fp} loaded.".format(fp=BAN_WORDS))
        return list
    except e:
        print("Ban list {fp} not found.".format(fp=BAN_WORDS))
        print("Proceeding without a ban list. Consider adding a ban list.")
        return []


def remove_inappropriate(df):
    """Removes item if description contains a ban word."""
    ban_list = load_ban_list()
    def filter(string):
        for bad_word in ban_list:
            if bad_word in string.lower():
                return False
        return True
    keep = df.apply(lambda x: filter(x.description + x.title), axis=1)
    return df.loc[keep, :]


def extract_fields(df):
    """Extracts relevant data from record."""
    df["images"] = df["Images"].apply(get_image_links)
    df["category_path_ids"] = df["category_path_ids"].apply(lambda x: x[0:2])
    df["category_id"] = df["category_path_ids"].apply(lambda x: x[-1])
    df["handmade"] = df["who_made"].apply(lambda x: x == "i_did")
    keep = [
        "title", "description", "price", "category_path_ids", "category_id",
        "materials", "images", "handmade", "num_favorers"
    ]
    return df.loc[:, keep]


def get_image_links(images):
    """Get image links from image data."""
    img_keys = ["url_75x75", "url_170x135", "url_570xN", "url_fullxfull"]
    return [dict([(k, v) for (k, v) in img.items() if k in img_keys]) for img in images]


def load_categories():
    """Loat Etsy categories."""
    categ = pd.read_json(CATEGORIES_FP)
    return categ


def get_categories(df):
    """Get only the categories present in the item df."""
    categ = load_categories()
    ids = set(sum(list(df["category_path_ids"]), []))
    keep = categ["category_id"].apply(lambda x: x in ids)
    cols = ["category_id", "page_title", "page_description", "short_name", "parent"]
    return categ.loc[keep, cols]


def main():
    """Prepares seeder data from Etsy dataset."""
    if os.path.exists(SAVE_PATH_TOP_ITEMS):
        print("Loading pre-compiled df of top items (500+ likes)")
        top_items = pd.read_pickle(SAVE_PATH_TOP_ITEMS)
    else:
        print("Getting items with 500+ likes from each json file.")
        top_items = get_top_items()
        save_top_items(top_items)
        print("Saved to {fp}".format(fp=SAVE_PATH_TOP_ITEMS))
    top_items = remove_inappropriate(top_items)
    top_items = extract_fields(top_items)
    top_items = top_items.sample(n=KEEP_ITEMS)
    top_items.to_json(SAVE_PATH_FINAL_ITEMS, orient="records")
    print("Saved sample of {n} items to {fp}".format(
        fp=SAVE_PATH_FINAL_ITEMS, n=KEEP_ITEMS
    ))
    categories = get_categories(top_items)
    categories.to_json(SAVE_PATH_FINAL_CATEGORIES, orient="records")
    print("Saved sample categories to {fp}".format(
        fp=SAVE_PATH_FINAL_CATEGORIES
    ))


if __name__ == "__main__":
    main()
