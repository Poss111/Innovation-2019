#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 15 20:46:57 2019

@author: poss
"""

from flask import Flask, request
from flask_restful import Resource, Api
from flask_jsonpify import jsonify
from json import dumps
from FileParser import simpleparser
import pandas as pd

simpleparser.loadValues()
app = Flask(__name__)
api = Api(app)

class test(Resource):
    def get(self):
        return jsonify(simpleparser.getPdOne().to_json())
    
class dealData(Resource):
    def get(self):
        return jsonify(simpleparser.getPdOne()["REFERENCE POOL ID"].unique().tolist())
    
api.add_resource(test, "/data-set")
api.add_resource(dealData, "/deal-ids")

if __name__ == "main":
    app.run(port=8080)