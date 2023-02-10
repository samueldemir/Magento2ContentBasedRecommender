"""
This file contains the MagentoAttributeHandler
"""
import requests
from MagentoConnectionHandler import MagentoConnectionHandler


class MagentoAttributeHandler(MagentoConnectionHandler):
    """
    This class handles the Magento Attributes.
    """

    def __init__(self):
        super(MagentoAttributeHandler, self).__init__("MagentoConnectionConf.yaml")

    def get_attributes(self):
        """
        This function returns the attributes of Magento.
        :return:
        :rtype:
        """
        endpoint = r"/rest/all/V1/products/attributes"
        params = {"searchCriteria[currentPage]": "0"}
        get_response = requests.get(url=self.base_url + endpoint,
                                    auth=self.magento_auth,
                                    params=params
                                    )

        get_response.raise_for_status()
        attributes = get_response.json()

        return attributes

    def get_attribute_ids_and_values(self):
        """
        This function returns a nested dict per attribute_code.
        Then key is the value id and the correspondig label name per id.
        :return:
        :rtype:
        """

        attributes = self.get_attributes()
        attribute_id_value_dict = {}
        attribute_types_to_save = ["select", "multiselect"]
        for attribute in attributes["items"]:
            attribute_code = attribute["attribute_code"]
            frontend_input = attribute["frontend_input"]
            if frontend_input in attribute_types_to_save:
                attribute_code_dict = {}
                options = attribute["options"]
                for option in options:
                    label = option["label"]
                    if label == " ":
                        continue
                    else:
                        identifier = option["value"]
                        attribute_code_dict[identifier] = label
                attribute_id_value_dict[attribute_code] = attribute_code_dict

        return attribute_id_value_dict

    def get_attribute_types(self):
        """
        This function returns the attribute types as values to its corresponding attribute_codes as keys.
        :return:
        :rtype:
        """
        attributes = self.get_attributes()
        attribute_code_type_dict = {}
        for attribute in attributes["items"]:
            attribute_code = attribute["attribute_code"]
            frontend_input = attribute["frontend_input"]
            attribute_code_type_dict[attribute_code] = frontend_input

        return attribute_code_type_dict

    def get_category_id_and_names(self):
        """
        This function return a dict with category ids as keys and the names as values.

        :return:
        :rtype:
        """
        endpoint = r"/rest/all/V1/categories"
        params = {"searchCriteria[currentPage]": "0"}
        get_response = requests.get(url=self.base_url + endpoint,
                                    auth=self.magento_auth,
                                    params=params
                                    )

        get_response.raise_for_status()
        category_names = get_response.json()

        id_name_pair = {}
        for elem in category_names["children_data"]:
            if len(elem) > 0:
                identifier = elem["id"]
                name = elem["name"]
                id_name_pair[identifier] = name
            self.find_children(elem, id_name_pair)

        return id_name_pair

    def find_children(self, elem, id_name_pair):
        """
        This function find the children through the child_category key.
        :param elem:
        :type elem:
        :param id_name_pair:
        :type id_name_pair:
        :return:
        :rtype:
        """

        for elem in elem["children_data"]:
            if len(elem) > 0:
                identifier = str(elem["id"])
                name = elem["name"]
                id_name_pair[identifier] = name
                self.find_children(elem, id_name_pair)
