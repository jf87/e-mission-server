import emission.core.get_database as edb
import pandas as pd
import requests
import copy
import json
import enum
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt
from transport_co2 import estimate_co2
from datetime import datetime
import uuid
import requests
import copy
import json
from uuid import UUID
import arrow


class PredictedModeTypes(enum.IntEnum):
    UNKNOWN = 0
    WALKING = 1
    BICYCLING = 2
    BUS = 3
    TRAIN = 4
    CAR = 5
    AIR_OR_HSR = 6
    SUBWAY = 7
    TRAM = 8
    LIGHT_RAIL = 9


mode_mapping = {
    PredictedModeTypes.UNKNOWN: "walk",
    PredictedModeTypes.WALKING: "walk",
    PredictedModeTypes.BICYCLING: "walk",
    PredictedModeTypes.BUS: "bus",
    PredictedModeTypes.TRAIN: "light_rail",
    PredictedModeTypes.CAR: "small_car",
    PredictedModeTypes.AIR_OR_HSR: "airplane",
    PredictedModeTypes.SUBWAY: "subway",
    PredictedModeTypes.TRAM: "light_rail",
    PredictedModeTypes.LIGHT_RAIL: "light_rail",
}


def compute_carbon_footprint(x):
    mode = mode_mapping[x.sensed_mode]
    return estimate_co2(mode=mode, distance_in_km=x.distance / 1000)


ngsi_template = {
    "id": "urn:section1",
    "type": "SectionObserved",
    "transportMode": {
        "type": "Property",
        "observedAt": "2021-03-24T12:10:00Z",
        "value": "car",
    },
    "distance": {
        "type": "Property",
        "observedAt": "2021-03-24T12:10:00Z",
        "value": 3464,
    },
    "duration": {
        "type": "Property",
        "observedAt": "2021-03-24T12:10:00Z",
        "value": 123,
    },
    "speed": {"type": "Property", "observedAt": "2021-03-24T12:10:00Z", "value": 40},
    "co2": {"type": "Property", "observedAt": "2021-03-24T12:10:00Z", "value": 123},
    "location": {
        "type": "GeoProperty",
        "value": {
            "type": "LineString",
            "coordinates": [[139.815535, 35.772622999999996], [139.815535, 35.774623]],
        },
    },
    "@context": [
        {
            "transportMode": "odala:transportMode",
            "distance": "odala:distance",
            "duration": "odala:duration",
            "co2": "odala:co2",
        },
        "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
    ],
}


def create_payloads(is_df):
    payloads = []
    for index, row in is_df.iterrows():
        rnd_distance = np.random.uniform(200, 400)
        payload = copy.deepcopy(ngsi_template)
        observedAt = datetime.utcfromtimestamp(row["end_ts"]).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        payload["id"] = "urn:" + str(row["cleaned_section"])
        payload["transportMode"]["value"] = PredictedModeTypes(row["sensed_mode"]).name
        payload["transportMode"]["observedAt"] = observedAt
        payload["distance"]["value"] = row["distance"]
        payload["distance"]["observedAt"] = observedAt
        payload["duration"]["value"] = row["duration"]
        payload["duration"]["observedAt"] = observedAt
        payload["speed"]["value"] = row["speed"]
        payload["speed"]["observedAt"] = observedAt
        payload["co2"]["value"] = row["co2"]
        payload["co2"]["observedAt"] = observedAt
        payload["location"]["value"]["coordinates"] = [
            add_randomness(row["start_loc"]["coordinates"], rnd_distance),
            add_randomness(row["end_loc"]["coordinates"], rnd_distance),
        ]
        payloads.append(payload)
    return payloads


def post_payloads(payloads):
    # url = 'http://cema.nlehd.de:2042/ngsi-ld/v1/entityOperations/upsert'
    url = "http://cema.nlehd.de:2042/ngsi-ld/v1/entities"
    headers = {"Content-Type": "application/ld+json"}
    for e in payloads:
        r = requests.post(url, data=json.dumps(e), headers=headers)
        if r.status_code != 201:
            print("request failed:", r.status_code)
            print(r.text)


import numpy as np


from math import radians, cos, sin, asin, sqrt


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r


def add_randomness(coordinates, distance):
    """
            Utility method for simulation of the points
    """
    x0 = coordinates[0]
    y0 = coordinates[1]
    r = distance / 111300
    u = np.random.uniform(0, 1)
    v = np.random.uniform(0, 1)
    w = r * np.sqrt(u)
    t = 2 * np.pi * v
    x = w * np.cos(t)
    x1 = x / np.cos(y0)
    y = w * np.sin(t)
    return [x0 + x1, y0 + y]


def delete_data(all_users):
    new_data = False
    for user in all_users:
        ts = esta.TimeSeries.get_time_series(user["uuid"])
        start = arrow.get(
            "2019-01-01"
        ).timestamp  # arrow.utcnow().float_timestamp-(3600*6)
        end = arrow.utcnow().float_timestamp
        tq = estt.TimeQuery("data.start_ts", start, end)
        is_df = ts.get_data_df("analysis/inferred_section", time_query=tq)
        if is_df.empty:
            continue
        new_data = True
        for index, row in is_df.iterrows():
            entity_id = "urn:" + str(row["cleaned_section"])
            r = requests.delete(
                "http://cema.nlehd.de:2042/ngsi-ld/v1/temporal/entities/" + entity_id,
                headers={"Content-Type": "application/ld+json"},
            )
            print(r)
            r = requests.delete(
                "http://cema.nlehd.de:2042/ngsi-ld/v1/entities/" + entity_id,
                headers={"Content-Type": "application/ld+json"},
            )
            print(r)
    if not new_data:
        print("Did not find any new data")


if __name__ == "__main__":

    all_users = list(edb.get_uuid_db().find({}, {"user_email": 1, "uuid": 1, "_id": 0}))
    new_data = False
    for user in all_users:
        ts = esta.TimeSeries.get_time_series(user["uuid"])
        start = arrow.utcnow().float_timestamp - (3600 * 24)
        end = arrow.utcnow().float_timestamp
        tq = estt.TimeQuery("data.start_ts", start, end)
        is_df = ts.get_data_df("analysis/inferred_section", time_query=tq)
        if is_df.empty:
            continue
        new_data = True
        is_df["speed"] = is_df.distance / is_df.duration
        is_df["co2"] = is_df.apply(compute_carbon_footprint, axis=1)
        payloads = create_payloads(is_df)
        post_payloads(payloads)
    if not new_data:
        print("Did not find any new data")
