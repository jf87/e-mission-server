import unittest
import json
#from utils import load_database_json, purge_database_json
#from main import tripManager
from pymongo import MongoClient
import logging
from get_database import get_db, get_mode_db, get_section_db, get_trip_db
import re
import sys 
import os
from datetime import datetime, timedelta
from recommender import utility_model_pipeline as pipeline
# Needed to modify the pythonpath
sys.path.append("%s/../CFC_WebApp/" % os.getcwd())
sys.path.append("%s" % os.getcwd())
from dao.user import User
from dao.client import Client
import tests.common
from moves import collect
from recommender.user_utility_model import UserUtilityModel

logging.basicConfig(level=logging.DEBUG)

class TestUtilityModelPipeline(unittest.TestCase):
  def setUp(self):
    self.testUUID = "myuuidisverylongandcomplicated"
    #self.testUserEmails = ["test@example.com", "best@example.com", "fest@example.com",
    #                       "rest@example.com", "nest@example.com"]
    self.serverName = 'localhost'

    self.testUsers = []

    #for userEmail in self.testUserEmails:
    #  User.register(userEmail)
    #  self.testUsers += [User.fromEmail(section['user_id'])] # can access uuid with .uuid

    # Sometimes, we may have entries left behind in the database if one of the tests failed
    # or threw an exception, so let us start by cleaning up all entries
    self.ModesColl = get_mode_db()
    self.ModesColl.remove()

    self.assertEquals(self.ModesColl.find().count(), 0)

    dataJSON = json.load(open("tests/data/modes.json"))
    for row in dataJSON:
      self.ModesColl.insert(row)
    
    #TODO: add many trip filter functions to play with
    self.trip_filters = None

    # import data from tests/data/testModeInferFiles
    #self.pipeline = pipeline.ModeRecommendationPipeline()
    #self.testRecommendationPipeline()
    # register each of the users and add sample trips to each user
    result = self.loadTestJSON("tests/data/missing_trip")
    collect.processResult(self.testUUID, result)

  def tearDown(self):
    get_section_db().remove({"user_id": self.testUUID})
    self.ModesColl.remove()
    self.assertEquals(self.ModesColl.find().count(), 0)
    #for testUser in self.testUsersEmails:
    #  purge_database_json.purgeData('localhost', testUser)

  def loadTestJSON(self, fileName):
    fileHandle = open(fileName)
    return json.load(fileHandle)
    
  def testRetrieveTrainingTrips(self):
    #get a users trips, there should be 21
    trip_list = pipeline.get_training_trips(self.testUUID, self.trip_filters)
    self.assertEquals(len(list(trip_list)), 22) 

  def testBuildUserModel(self):
    #get a users trips, there should be 21
    trip_list = pipeline.get_training_trips(self.testUUID, self.trip_filters)
    model = pipeline.build_user_model(self.testUUID, trip_list)
    self.assertTrue(isinstance(model, UserUtilityModel))

  def testModifyUserModel(self):
    trip_list = pipeline.get_training_trips(self.testUUID, self.trip_filters)
    model = pipeline.build_user_model(self.testUUID, trip_list)
    print model
    self.assertTrue(isinstance(model, UserUtilityModel))
    new_model = pipeline.modify_user_utility_model(self.testUUID, model, trip_list)
    self.assertTrue(isinstance(model, UserUtilityModel))
    self.assertNotEquals(new_model, model)

if __name__ == '__main__':
    unittest.main()
