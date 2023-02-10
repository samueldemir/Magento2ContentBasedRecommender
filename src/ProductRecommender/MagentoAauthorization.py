"""
File that connects to Magento2
"""
from requests_oauthlib import OAuth1


class Authorizator:
    """
    Authorization Class
    """

    def __init__(self, consumer_key, consumer_key_secret, access_token, access_token_secret):
        self.consumer_key = consumer_key
        self.consumer_key_secret = consumer_key_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret

    def get_authorization_object(self) -> OAuth1:
        """
        A function that returns the authorization
        :return: An authorization object.
        :rtype: OAuth1
        """

        magento_auth = OAuth1(self.consumer_key,
                              client_secret=self.consumer_key_secret,
                              resource_owner_key=self.access_token,
                              resource_owner_secret=self.access_token_secret)

        return magento_auth
