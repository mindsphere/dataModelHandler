#! /bin/env python


#Inputs

"""
From and To Timestamp
SendInterval in minutes
Mode:
    square;amp:5;freq:20
    pulse;amp:-2;freq:0.1
    cos;amp:-2;freq:0.1
    sin;amp-1;freq:1
    randomInt:-25 to:80
    randomDouble:0 to 1000
    randomBool:80% (likelihood for true)
    randomString: [list of strings to chose from]

A dictionary would some of those keys, not all are mandatory
{
from:
to:
sendInterval:
mode:
min:
max:
likelihoodForTrue:
listOfWords
}    
"""
# Returns
"""
An Object with the simulated timeseries data, containing tuples with Timestamp, Value
"""

import pandas as pd
import math
import time
import json
import os
import random
import string
import itertools
import traceback
from datetime import datetime
from string import Template



inputDictionary = {
       "startDate" : datetime(2021, 1, 15),
       "endDate": datetime(2021, 1, 16),
       "dataCreationInterval" : "1min",
       "seriesDefinitions": 
            {
           "variablename1":
                {
               "mode": "randomString",
               "listOfStrings":["This","is","a","dummy","stringlist","lama"]
                },
           "variablename2":
               {
               "mode": "randomBool",
               "likelihoodForTrueInPercent": 75,
               },
               
           "variablename3":
               {
               "mode":"randomInt",
               "min":2,
               "max":15,           
               },
               
           "variablename4":
               {
               "mode":"sin",
               "waveFormDescription": {"frequencyInHz":0.001, "amplitude":10}
               }
            }
}

    
def squareWave(value, mode ="square"): #supports two modes: "square" (dropping down to -1) and "pulse" (dropping down to 0)
    if value >= 0:
        return 1
    elif mode == "pulse":
        return -1
    else:
        return 0
  
  

def getSimulatedData(inputDictionary,timestampsAsString = False):
    try:
        timeTuples = []
        startDate = inputDictionary["startDate"]
        endDate = inputDictionary["endDate"]
        dataCreationInterval = inputDictionary["dataCreationInterval"]
        timeRange = pd.date_range(startDate, endDate, freq=dataCreationInterval).to_pydatetime().tolist()
        seriesDefinitions =  inputDictionary.get("seriesDefinitions")
        
        simulatedData = {}
        
        if not seriesDefinitions:
            seriesDefinitions = {}
            seriesDefinitions["dummyName"] = inputDictionary
            
        
        
        for seriesName in seriesDefinitions:
            inputDictionary = seriesDefinitions[seriesName]

            timeStampValuePairs = {}
                
            mode = inputDictionary["mode"]

            if mode in ("square","pulse","sin","cos"):      
                waveFormDescription = inputDictionary.get("waveFormDescription")
                if waveFormDescription:
                    frequency = waveFormDescription["frequencyInHz"]        
                    amplitude = waveFormDescription["amplitude"]       
                        
                    firstValue =  timeRange[0]
                    timeStampValuePairs[firstValue] = 1 if "mode" =="cos" else 0
                    for timestamp in timeRange[1:]:
                        timePassedInSeconds = (timestamp - firstValue).total_seconds() 
                        

                        if  mode == "sin":
                            simulatedValue = amplitude * math.sin(2 * math.pi * frequency * timePassedInSeconds)
                        if mode == "cos":
                            simulatedValue = amplitude * math.sin(2* math.pi * frequency * timePassedInSeconds)
                        if mode in ("square","pulse"):
                            simulatedValue = amplitude * squareWave(math.sin(2* math.pi * frequency * timePassedInSeconds), mode = "square")
                        
                        timeStampValuePairs[timestamp] = simulatedValue
                else:
                    print('No Wave Form description has been found in the input dictionary, though a periodical simulation has been chosen (here: %s).\nPlease add such a definition using this syntax\n"waveFormDescription": {"frequencyInHz":0.001, "amplitude":10}' %mode)
                    return []
            else:        
                for timestamp in timeRange:
                    if mode == "randomInt":
                        min = inputDictionary["min"]
                        max = inputDictionary["max"]
                        simulatedValue = random.randint(min, max)
                    
                    if mode == "randomDouble":
                        min = inputDictionary["min"]
                        max = inputDictionary["max"]
                        simulatedValue = random.uniform(min, max)
                        
                    if mode == "randomBool":
                        likelihoodForTrueInPercent = inputDictionary["likelihoodForTrueInPercent"]
                        simulatedValue = random.random() * 100 <= likelihoodForTrueInPercent
                        
                    if mode =="randomString":
                        listOfStrings = inputDictionary["listOfStrings"]
                        simulatedValue = random.choice(listOfStrings)
                    
                    timeStampValuePairs[timestamp] = simulatedValue
                    
            simulatedData[seriesName] = timeStampValuePairs
            
        #join all series in one dict
        resultDictionary = {}
        for timestamp in timeRange:
        
            if timestampsAsString:
                timestampInTargetFormat = timestamp.isoformat()+"Z"
            else:
                timestampInTargetFormat = timestamp            
            currentDict = {}
            for seriesName in simulatedData:
                currentDict[seriesName] = simulatedData[seriesName][timestamp]
            resultDictionary[timestampInTargetFormat] = currentDict
            
        return(timeTuples)
        
        
    except:
        print(f"Something went wrong when trying to simulate data. Input dictionary has been {inputDictionary}:")
        traceback.print_exc()
        print("Return an empty list now")
        
        return []

#getSimulatedTuples(inputDictionary)
 
def getXValuesForWaveform(frequency):

    period = 1/frequency
    stepSize = math.ceil(period*360)
    currentWaveGenerationRange = list(range(0,361,stepSize))
    return currentWaveGenerationRange



