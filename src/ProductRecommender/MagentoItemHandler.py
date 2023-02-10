"""
This file is for handling the Magento2 Items and get the releveant informations
"""

from typing import List, Dict

import pandas as pd
import requests
from MagentoAttributeHandler import MagentoAttributeHandler
from MagentoConnectionHandler import MagentoConnectionHandler
from util import get_config_file


class MagentoItemHandler(MagentoConnectionHandler):
    """
    A Magento2 Item Handler Class

    """

    def __init__(self):
        super(MagentoItemHandler, self).__init__("MagentoConnectionConf.yaml")
        self.magento_attribute_handler = MagentoAttributeHandler()
        self.attribute_id_value = self.magento_attribute_handler.get_attribute_ids_and_values()
        self.attribute_conf_file = get_config_file("MagentoAttributeConf.yaml")

    def get_products(self) -> List[Dict]:
        """
        This functions collects the product that are existing on magento.
        Returns: A List with product items.

        """

        full_endpoint = self.base_url + "/rest/all/V1/products"

        params = {"searchCriteria[currentPage]": "0"}
        get_resp = requests.get(url=full_endpoint,
                                auth=self.magento_auth,
                                params=params
                                )

        get_resp.raise_for_status()
        product_items = get_resp.json()["items"]

        return product_items

    def get_product_attribute_frame(self):
        """
        This fucntion returns the products with all corresponding attribute values.
        :return:
        :rtype:
        """
        magento_products = self.get_products()

        products_list = []
        product_dict = {}
        attribute_code_type_dict = self.magento_attribute_handler.get_attribute_types()
        attribute_id_value_dict = self.magento_attribute_handler.get_attribute_ids_and_values()
        category_id_names = self.magento_attribute_handler.get_category_id_and_names()
        for item in magento_products:
            product_dict["id"] = item["id"]
            product_dict["sku"] = item["sku"]
            product_dict["name"] = item["name"]
            product_dict["price"] = item["price"]
            product_dict["status"] = item["status"]
            product_dict["type_id"] = item["type_id"]
            product_dict["weight"] = item.get("weight", None)

            attribute_types_to_convert_ids = ["select", "multiselect"]
            for custom_attribute in item["custom_attributes"]:
                attribute_code = custom_attribute['attribute_code']
                attribute_value_identifier = custom_attribute["value"]
                if attribute_code_type_dict[attribute_code] in attribute_types_to_convert_ids:
                    attribute_value_identifier = attribute_value_identifier.split(
                        ",") if attribute_value_identifier != "" else attribute_value_identifier
                    attribute_value = ",".join(
                        [attribute_id_value_dict[attribute_code][x] for x in attribute_value_identifier])
                elif attribute_code == "category_ids":
                    attribute_value = ",".join(attribute_value_identifier)
                    product_dict["category_names"] = ",".join(
                        category_id_names[ident] for ident in attribute_value_identifier)
                else:
                    attribute_value = attribute_value_identifier

                product_dict[attribute_code] = [attribute_value]

            products_list.append(pd.DataFrame(product_dict))
            product_dict.clear()

        full_df_without_qty = pd.concat(products_list, ignore_index=True)

        qty_df = self.get_qtys()
        values = {"qty": 99999999999}
        full_df_with_qtys = pd.merge(full_df_without_qty, qty_df, on='id', how='left').fillna(value=values)

        return full_df_with_qtys

    def get_qtys(self) -> pd.DataFrame:
        """
        This fucntion returns the qtys per product.
        The qty param is a threshold and return all products with less qty as param is set.
        Page Size selects the number of products returned.
        Both params needs to get increased to get all product qtys.
        scope id is set to zero.
        """

        full_endpoint = self.base_url + "/rest/all/V1/stockItems/lowStock"

        unlimited_product_count = 999999999999999
        params = {"pageSize": unlimited_product_count,
                  "qty": unlimited_product_count,
                  "scopeId": 0}
        get_resp = requests.get(url=full_endpoint,
                                auth=self.magento_auth,
                                params=params
                                )

        get_resp.raise_for_status()
        product_qtys = get_resp.json()["items"]
        dfs = []
        for elem in product_qtys:
            item_id = elem["product_id"]
            qty = elem["qty"]
            dfs.append(pd.DataFrame({"id": [item_id], "qty": [qty]}))
        qty_df = pd.concat(dfs, ignore_index=True)

        return qty_df
