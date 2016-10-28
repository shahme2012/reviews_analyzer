# -*- coding: utf-8 -*-
import os, sys
import json
import csv
import re

from collections import OrderedDict

os.environ['SPARK_HOME'] = "/{path_to_spark}/Spark/spark-2.0.1-bin-hadoop2.7"
os.environ['PYSPARK_PYTHON'] = 'python3'
sys.path.append("/{path_to_spark}/Spark/spark-2.0.1-bin-hadoop2.7/python")

try:
    from pyspark import SparkContext
    from pyspark import SparkFiles
    from pyspark.sql import SQLContext
except ImportError as e:
    print("Can not import Spark Modules", e)
    sys.exit(1)


def get_words_count(text: str):
    """
    :param text
    :return number of words per text
    """
    word_regex = r'\b\w+\b'
    list_of_words = re.findall(word_regex, text)
    return len(list_of_words)


def add_words_amount(review: str):
    """
    :param review: jsonString
    :return: dictionary with additional word_amount item
    """
    json_decoded = json.loads(review, object_pairs_hook=OrderedDict)
    review_text = json_decoded["text"]
    text_len = get_words_count(review_text)
    json_decoded.update({"words_amount": text_len})
    return json_decoded


def check_item(item: tuple):
    """
    :param item: tuple of dictionary key-value
    :return: tuple of key-value if value is not dict, else check dictionary
    """
    key, value = item
    return extract_nested_dict(value) if isinstance(value, dict) else (key, value)


def extract_nested_dict(dictionary: dict):
    return list(map(check_item, dictionary.items()))


def normalize_dict(dictionary):
    """
    :param dictionary: dict
    :return: ordered dictionary
    """
    css_row = []
    for value in extract_nested_dict(dictionary):
        if (isinstance(value, list)):
            css_row.extend(value)
        else:
            css_row.append(value)
    return OrderedDict(css_row)


def list_of_dicts_to_csv(list_of_dicts, file_name):
    title = list_of_dicts[0].keys()
    with open(file_name, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=title)
        writer.writeheader()
        for data in list_of_dicts:
            writer.writerow(data)


if __name__ == "__main__":
    sc = SparkContext(appName="FullBiographiesProcessor")

    reviews = sc.textFile("./yelp_academic_dataset_review.json") \
        .map(add_words_amount)

    normalizedRDD = reviews.map(normalize_dict)

    useful = normalizedRDD.top(10, key=lambda items: items["useful"])
    funny = normalizedRDD.top(10, key=lambda items: items["funny"])
    cool = normalizedRDD.top(10, key=lambda items: items["cool"])
    verbose = normalizedRDD.top(10, key=lambda items: items["words_amount"])

    list_of_dicts_to_csv(useful, "funny.csv")
    list_of_dicts_to_csv(funny, "useful.csv")
    list_of_dicts_to_csv(cool, "cool.csv")
    list_of_dicts_to_csv(verbose, "verbose.csv")

    sc.stop()
