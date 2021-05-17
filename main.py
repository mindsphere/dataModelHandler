
import json
import datetime, traceback
import os
import sys
import traceback
import csv
import importlib
sys.path.append("./modules")

import modules.readConfig as config

import helpers

#######################################
############## ABSTRACT ###############
#######################################

# This module is the general orchestrator for the whole process
# It calls various modules and collects the returned information 

#######################################
########### MODULE CONFIG #############
#######################################

standardMessage =  """
        Welcome to the MindSphere Data Model Handler.
        Please provide any feedback regarding bugs/issues/requests on the repositry
        """


#######################################
######### HELPER FUNCTIONS ############
#######################################

#######################################
def postProcessConfig():
    config.defaultAssetType = helpers.deriveIdFromNameOrId(config.defaultAssetType)


#######################################

def displayWrappedMultilineMsg(message):
            
    print("\n")
    print("$"*120)
    print('\n'.join('{:^110}'.format(s) for s in message.split('\n')))
    print("$"*120)
    print("\n")
    print("="*80)
#######################################


#######################################
############ MAIN BLOCK ###############
#######################################
postProcessConfig()
currentModul = importlib.import_module(config.toolMode)

message = standardMessage
displayWrappedMultilineMsg(message) 

currentModul.start() 