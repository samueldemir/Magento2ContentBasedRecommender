"""
This file computes the cosine similarity of the products
"""
import numpy as np
from typing import Union

import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer

from MagentoItemHandler import MagentoItemHandler
from MySqlHandler import MySqlHandler
from util import get_config_file


def change_category_name_string(input_str: Union[str, float], split_by=",") -> str:
    """

    :param split_by:
    :param input_str:
    :return:
    :rtype:
    """

    splitted_categorys = input_str.split(split_by)
    new_cat_names = []
    for cat in splitted_categorys:
        new_cat_name = "<" + cat + ">"
        new_cat_names.append(new_cat_name)

    return " ".join(new_cat_names)


def remove_simple_from_its_configurable(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove the simple products if they have configurable or bundle as parent.
    We want to reocmmend only visible products!
    :param input_df:
    :type input_df:
    :return:
    :rtype:
    """

    input_df["esg_sku_tmp"] = (input_df.loc[:, "sku"].str.split("-").str[2])
    config_groups = input_df.groupby(["esg_sku_tmp"]).ngroup()
    input_df["group_id"] = config_groups

    count_groups = config_groups.unique().tolist()
    df_unions = []
    for group_id in count_groups:

        df_cur_group = input_df[input_df["group_id"] == group_id].copy().reset_index(drop=True)
        sum_qtys = df_cur_group.loc[:, "qty"].sum()
        df_cur_group["summed_qtys"] = sum_qtys
        product_types_in_group = set(df_cur_group["type_id"].tolist())

        if any(word in product_types_in_group for word in ["bundle", "configurable"]):
            df_cur_group = df_cur_group[df_cur_group["type_id"] != "simple"]

        df_unions.append(df_cur_group)

    cleaned_df = pd.concat(df_unions, ignore_index=True, sort=False)

    return cleaned_df


def create_price_ranges(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df["esg_sku_tmp"] = (input_df.loc[:, "sku"].str.split("-").str[2])
    config_groups = input_df.groupby(["esg_sku_tmp"]).ngroup()
    input_df["group_id"] = config_groups

    count_groups = config_groups.unique().tolist()
    df_unions = []

    price_ranges = {"price_range_1": "0-200",
                    "price_range_2": "200-500",
                    "price_range_3": "500-1500",
                    "price_range_4": "1500-3000",
                    "price_range_5": "3000-5000",
                    "price_range_6": "5000-9999999"
                    }

    for group_id in count_groups:
        df_cur_group = input_df[input_df["group_id"] == group_id].copy().reset_index(drop=True)
        price_lst = df_cur_group.loc[:, "price"].to_list()

        price_ranges_found = []
        for price in price_lst:

            for k, v, in price_ranges.items():
                min_range = int(v.split("-")[0])
                max_range = int(v.split("-")[1])

                if min_range <= price < max_range:
                    price_ranges_found.append(k)
                    break
        price_ranges_found_str = " ".join(price_ranges_found)
        df_cur_group["price_ranges"] = price_ranges_found_str
        df_unions.append(df_cur_group)

    price_ranges_df = pd.concat(df_unions, ignore_index=True, sort=False)

    return price_ranges_df


magento_item_handler = MagentoItemHandler()
product_df_raw = magento_item_handler.get_product_attribute_frame()

product_df_price_ranges = create_price_ranges(product_df_raw)

product_df_without_simple_conf = remove_simple_from_its_configurable(product_df_price_ranges)

# Remove out of stock
product_df = product_df_without_simple_conf[~(
        (product_df_without_simple_conf["status"] == 2) | (product_df_without_simple_conf["summed_qtys"] == 0))]

product_df["category_names"] = product_df.loc[:, "category_names"].fillna("").apply(change_category_name_string,
                                                                                    split_by=",")
product_df["esg_produktgruppe"] = product_df.loc[:, "esg_produktgruppe"] \
    .fillna("") \
    .apply(change_category_name_string, split_by=",")

product_df["esg_product_name_keywords"] = (product_df
                                           .loc[:, "esg_product_name_keywords"]
                                           .fillna("")
                                           .apply(change_category_name_string, split_by="|"))
product_df["esg_farben"] = product_df.loc[:, "esg_farben"].fillna("").apply(change_category_name_string,
                                                                            split_by=",")

product_df["imp"] = product_df.loc[:, "short_description"].str.replace(",", "") \
                    + " " + product_df.loc[:, "esg_hersteller"] \
                    + " " + product_df.loc[:, "category_names"] + " " + product_df.loc[:, "esg_produktgruppe"] \
                    + " " + product_df.loc[:, "esg_farben"] + " " + product_df.loc[:, "esg_produktsparte"] \
                    + " " + product_df.loc[:, "esg_product_name_keywords"] \
                    + " " + product_df.loc[:, "price_ranges"]

product_df["ids"] = [i for i in range(0, product_df.shape[0])]

vec = TfidfVectorizer()
vecs = vec.fit_transform(product_df["imp"].apply(lambda x: np.str_(x)))

sim = cosine_similarity(vecs)


def recommend(esg_sku):
    """
    This function recommends to one product all other products in descending order.
    :param esg_sku:
    :type esg_sku:
    :return:
    :rtype:
    """
    sku_internal_id = product_df[product_df.sku == esg_sku]["ids"].values[0]
    scores = list(enumerate(sim[sku_internal_id]))
    sorted_scores = sorted(scores, key=lambda x: x[1], reverse=True)
    sorted_scores = sorted_scores[1:]
    skus = [product_df[esg_sku[0] == product_df["ids"]]["sku"].values[0] for esg_sku in sorted_scores]
    return skus


def recommend_n_products(sku_dupl, recommender_list, n_products=15):
    """
    This function collects from a given list of recommendations (list with descending order) the first n products.

    :param sku_dupl:
    :type sku_dupl:
    :param recommender_list:
    :type recommender_list:
    :param n_products:
    :type n_products:
    :return:
    :rtype:
    """
    first_n = []
    count = 0
    for esg_sku in recommender_list:
        if count >= n_products:
            break
        # Remove duplicates per products sku
        if sku_dupl == esg_sku:
            continue
        else:
            count += 1
            first_n.append(esg_sku)
    return first_n


sku_list = product_df.loc[:, "sku"].tolist()
product_recommender_dict = {}
for sku in sku_list:
    reommender_product_list = recommend(sku)
    recommender_products = recommend_n_products(sku, reommender_product_list, 20)
    product_recommender_dict[sku] = recommender_products

db_config = get_config_file("DBConf.yaml")
user = db_config["user"]
password = db_config["password"]
database = db_config["database"]
host = db_config["host"]
table_name = db_config["table_name"]

my_sql_handler = MySqlHandler(user,
                              password,
                              database,
                              host)

my_sql_handler.create_table_if_not_exists(table_name)
my_sql_handler.insert_data(table_name, product_recommender_dict)
