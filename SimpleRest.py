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
from flask import request
import constants
import dateparser
from flask_cors import CORS

simpleparser.loadValues() 
app = Flask(__name__)
cors = CORS(app, resources={r"*": {"origins": "*"}})
api = Api(app)

class test(Resource):
    def get(self):
        return jsonify(simpleparser.getPdOne().to_json())
    
class dealData(Resource):
    def get(self):
        return jsonify(simpleparser.getPdOne()[constants.REFERENCE_POOL_ID].unique().tolist())
    
class dealAggData(Resource):
    def get(self):
        return simpleparser.pdOne[simpleparser.pdOne[constants.REFERENCE_POOL_ID] == 1462].groupby([constants.MONTHLY_REPORTING_PERIOD],group_keys=True)[constants.CURRENT_ACTUAL_UPB].agg("sum").to_json()

class dealAggPercentChangeData(Resource):
    def get(self):
        return simpleparser.pdOne[simpleparser.pdOne[constants.REFERENCE_POOL_ID] == 1462].groupby([constants.MONTHLY_REPORTING_PERIOD],group_keys=True)[constants.CURRENT_ACTUAL_UPB].agg("sum").pct_change().to_json()

class timeline(Resource):
    def get(self):
        return jsonify(pd.to_datetime(simpleparser.pdOne[constants.MONTHLY_REPORTING_PERIOD].sort_values(ascending=True).unique(), format='%m%Y').strftime('%B %Y').tolist())
    
class dynamicColumnFetch(Resource):
    def get(self):
        columnList = []
        filterYear = dateparser.parse(str(request.args.get("reportingYear")))
        filterDealName = request.args.get("dealName")
        numberOfRows = int(request.args.get("numberOfRows")) if request.args.get("numberOfRows") is not None else 1000
        print("NumberOfRows to return :: " + str(numberOfRows))
        for requestParamKey in request.args:
            if ("column" in requestParamKey):
                print("Requesting following column :: " + request.args.get(requestParamKey))
                columnList.append(request.args.get(requestParamKey)) 
                
        if (filterDealName != None):
            print ("Filter Deal Name :: " + str(filterDealName))
            return jsonify(simpleparser.pdOne[simpleparser.pdOne[constants.DEAL_NAME] == filterDealName].filter(list(columnList)).head(numberOfRows).to_json(orient='records'))
        elif (filterYear != None):
            print("Filter Year :: " + str(filterYear))
            return jsonify(simpleparser.pdOne[simpleparser.pdOne[constants.MONTHLY_REPORTING_PERIOD] > filterYear].filter(list(columnList)).head(numberOfRows).to_json(orient='records'))
        else:
            return jsonify(simpleparser.pdOne.filter(list(columnList)).sample(n=numberOfRows).to_json(orient='records'))    
    
@app.errorhandler(500)    
def InternalServerError(RequestException: Exception):
    print("Failed call")
    return "Failed", 500

@app.errorhandler(404)    
def ApiNotFound(RequestException: Exception):
    print("Failed call - API Not Found")
    return "Not Found", 404 
    
api.add_resource(test, "/creditrisk/data")
api.add_resource(dealData, "/creditrisk/deals")
api.add_resource(dealAggData, "/creditrisk/deals-agg")
api.add_resource(dealAggPercentChangeData, "/creditrisk/deals-percent-agg")
api.add_resource(dynamicColumnFetch, "/creditrisk/column-fetch")
api.add_resource(timeline, "/creditrisk/fetchLGTime")

if __name__ == "main":
    app.errorhandler(InvalidUsage)
    app.run(port=8080)