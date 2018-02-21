from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import pymongo
import copy

import emission.core.get_database as edb
import emission.core.wrapper.section as ecws
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.modeprediction as ecwm

import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta


def get_sections_for_trip(user_id, trip_id):
    curr_query = {"user_id": user_id, "data.trip_id": trip_id}
    return _get_sections_for_query(curr_query, "data.start_ts")

def get_sections_for_trip_list(user_id, trip_list):
    curr_query = {"user_id": user_id, "data.trip_id": {"$in": trip_list}}
    return _get_sections_for_query(curr_query, "data.start_ts")

def _get_sections_for_query(section_query, sort_field):
    section_query.update({"metadata.key": "segmentation/raw_section"})
    logging.debug("Returning sections for query %s" % section_query)
    section_doc_cursor = edb.get_analysis_timeseries_db().find(
        section_query).sort(sort_field, pymongo.ASCENDING)
    logging.debug("result cursor length = %d" % section_doc_cursor.count())
    return [ecwe.Entry(doc) for doc in section_doc_cursor]

def get_inferred_mode_entry(user_id, section_id):
    prediction_key_query = {"metadata.key": "inference/prediction"}
    inference_query = {"user_id": user_id, "data.section_id": section_id}
    combo_query = copy.copy(prediction_key_query)
    combo_query.update(inference_query)
    logging.debug("About to query %s" % combo_query)
    ret_list = list(edb.get_analysis_timeseries_db().find(combo_query))
    # We currently have only one algorithm
    assert(len(ret_list) <= 1)
    if len(ret_list) == 0:
        logging.debug("Found no inferred prediction, returning None")
        return None
    
    assert len(ret_list) == 1, "Found ret_list of length %d, expected 1" % len(ret_list)
    curr_prediction = ecwe.Entry(ret_list[0])
    assert curr_prediction.data.algorithm_id == ecwm.AlgorithmTypes.SEED_RANDOM_FOREST, \
        "Found algorithm_id = %s, expected %s" % (curr_prediction.data.algorithm_id,
            ecwm.AlgorithmTypes.SEED_RANDOM_FOREST)
    return curr_prediction
