from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import json
import logging
import uuid
import requests

# Our imports
import emission.core.get_database as edb

def get_platform_query(platform):
    return {"curr_platform": platform}

def get_sync_interval_query(interval):
    return {"curr_sync_interval": interval}

def get_user_query(user_id_list):
    return {"user_id": {"$in": user_id_list}}

def combine_queries(query_list):
    combined_query = {}
    for query in query_list:
        combined_query.update(query)
    return combined_query

def get_matching_tokens(query):
    logging.debug("Getting tokens matching query %s" % query)
    ret_map = {"ios": [],
               "android": []}
    ret_cursor = edb.get_profile_db().find(query, {"_id": False, "device_token": True, "curr_platform": True})
    for i, entry in enumerate(ret_cursor):
        curr_platform = entry["curr_platform"]
        device_token = entry["device_token"]
        if curr_platform is None or device_token is None:
            logging.warning("ignoring entry %s due to None values" % entry)
        else:
            logging.debug("adding token %s to list for platform %s" % (device_token, curr_platform))
            ret_map[curr_platform].append(device_token)

    return ret_map

def get_matching_user_ids(query):
    logging.debug("Getting tokens matching query %s" % query)
    ret_cursor = edb.get_profile_db().find(query, {"_id": False, "user_id": True})
    mapped_list = [e.get("user_id") for e in ret_cursor]
    ret_list = [item for item in mapped_list if item is not None]
    return ret_list
