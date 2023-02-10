"""
This file handles the communiation and transport the data to mysql database.

"""
import mysql.connector
from typing import Dict
import logging

logger = logging.getLogger("MySqlHandler")


class MySqlHandler:
    """
    this class handles the MySql Connection and transfers data.
    """

    def __init__(self, mysql_user, mysql_pwd, db_name, host):
        self.mysql_user = mysql_user
        self.mysql_pwd = mysql_pwd
        self.db_name = db_name
        self.host = host
        self.connection = self.get_connection()
        self.cursor = self.connection.cursor()

    def get_connection(self):
        """
        get the mysql connection
        :return:
        :rtype:
        """

        try:
            connection = mysql.connector.connect(user=self.mysql_user,
                                                 password=self.mysql_pwd,
                                                 host=self.host,
                                                 database=self.db_name)

            return connection
        except ConnectionError:
            print("connection error")
            exit(1)

    def create_table_if_not_exists(self, table):
        """
        This function creates the table
        :return:
        :rtype:
        """
        mycursor = self.cursor
        mycursor.execute(
            f"CREATE TABLE IF NOT EXISTS {table} (sku VARCHAR(255) NOT NULL PRIMARY KEY, recommondations TEXT DEFAULT NULL)")

    @staticmethod
    def find_differences(remote_data: Dict, recommender_data: Dict):
        """
        This function checks if some values changes for the product recommender.
        """
        differences_to_add_update = {}
        for sku_recom, v_recom in recommender_data.items():
            if sku_recom in remote_data:
                # Check diff
                v_remote = remote_data[sku_recom]
                if v_recom != v_remote:
                    differences_to_add_update[sku_recom] = v_recom
            else:
                differences_to_add_update[sku_recom] = v_recom

        differences_to_delete = []
        for sku_remote, v_remote in remote_data.items():
            if sku_remote not in recommender_data:
                differences_to_delete.append(sku_remote)

        return differences_to_add_update, differences_to_delete

    def upsert_data(self, table, remote_data, recommender_data):
        """
        upsert
        :return:
        :rtype:
        """

        differences_to_add_update, differences_to_delete = self.find_differences(remote_data, recommender_data)

        for esg_sku, string_rep_recomm in differences_to_add_update.items():
            sql = f"INSERT INTO {table} (sku, recommondations) VALUES (%s, %s) ON DUPLICATE KEY UPDATE recommondations = VALUES(recommondations)"
            val = (esg_sku, string_rep_recomm)

            self.cursor.execute(sql, val)
            self.connection.commit()

        for esg_sku in differences_to_delete:
            sql = f"DELETE FROM {table} WHERE sku = %s"
            val = (esg_sku, )
            self.cursor.execute(sql, val)
            self.connection.commit()

    def insert_data(self, table, product_recommender_dict):
        """
        This function inserts the relevant data.
        :param table:
        :type table:
        :param product_recommender_dict:
        :type product_recommender_dict:
        :return:
        :rtype:
        """

        remote_data = self.get_remote_data(table)
        recommender_data = {}
        for k, v in product_recommender_dict.items():
            string_rep_recomm = "$$".join(v)
            recommender_data[k] = string_rep_recomm

        self.upsert_data(table, remote_data, recommender_data)

    def get_remote_data(self, table):
        """
        This function return the remote table.
        :param table:
        :type table:
        :return:
        :rtype:
        """
        logger.info("Getting remote data")

        mycursor = self.cursor
        mycursor.execute(
            f"SELECT * FROM {table}")
        myresult = mycursor.fetchall()

        remote_data = {}
        for row in myresult:
            remote_data[row[0]] = row[1]

        return remote_data
