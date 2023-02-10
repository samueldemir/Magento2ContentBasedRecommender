"""
This file contains the MagentoConnectionHandler
"""

from MagentoAauthorization import Authorizator
from util import get_config_file


class MagentoConnectionHandler:
    """
    This function handles the Magento2 connection.
    """

    def __init__(self, path_to_conf):
        self.magento_conn_settings = get_config_file(path_to_conf)
        self.base_url = self.magento_conn_settings["base_url"]
        self.magento_auth = self.get_connection_to_magento()

    def get_connection_to_magento(self):
        """
        This fucntion returns the connection object to Magento
        :return:
        :rtype:
        """
        consumer_key = self.magento_conn_settings["consumer_key"]
        consumer_key_secret = self.magento_conn_settings["consumer_key_secret"]
        access_token = self.magento_conn_settings["access_token"]
        access_token_secret = self.magento_conn_settings["access_token_secret"]

        magento_authorizator_object = Authorizator(consumer_key=consumer_key,
                                                   consumer_key_secret=consumer_key_secret,
                                                   access_token=access_token,
                                                   access_token_secret=access_token_secret)

        magento_authorizator = magento_authorizator_object.get_authorization_object()

        return magento_authorizator
