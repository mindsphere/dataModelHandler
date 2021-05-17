import requests
import uuid
import json
import datetime, traceback
import os
import sys
import configparser
import traceback
import csv
from pprint import pprint
import helpers
from helpers import ContinueHelper
import pdb
import modules.readConfig as config


#Custom Modules
import helpers
from modules.helpers import ToolMode
from modules.helpers import SimpleError

from modules.mindsphereDataModelManager import MindsphereDataModelManager, importAssetsWithoutCheckingAnything, importAssetTypesWithoutCheckingMuch, createAndOnboardAgents
from modules.mindsphereApiCollection import createNewAssetInMindSphere, createNewAspectInMindSphere, createNewAssetTypeInMindSphere, createDatapointMapping, applyMappingConfigurationToDevice
from modules.datamodelClassDefinitions import Asset, AssetType, Aspect, ImportStatus
from modules.rowProcessor import convertRowsToClasses, extractAssetDefinitions, extractAspectDefinitions, extractAssetTypeDefinitions, extractAgentDefinitions,extractAndAttachDataPointDefinitions,createVirtualTargetAssetsFromDict
####################################### 
########### MODULE CONFIG #############
#######################################

# The following block loads parameters from the config an provides them in an easy to use way in this modul:
# Instead of config.<parametername> you can just use <parametername> as standalone afterwards

thisModule = sys.modules[__name__]
requiredParamters= "logging, inputFolderWithAgentDefinitions, tenantname, agentImportDefintionFile, defaultParentAssetName, parentIdsToBeExported, exportMode, exportedDataOutputFile,  exportAgentAndMappingConfiguration, convertToNeutralIds"
config.setSimpleConfigParametersToModule(thisModule, requiredParamters)

#Get Root Directory for Agents
rootFolderWithAgentDefinitions = inputFolderWithAgentDefinitions


#######################################
######### HELPER FUNCTIONS ############
#######################################


#######################################
############ MAIN BLOCK ###############
#######################################




def start():

    #############
    # Extract all assets (and optionally asset-types and aspects) from Mindsphere
    print("="*80)
    print("Initialize MindSphere data model now...")
    
    mindsphereDataModelManager =  MindsphereDataModelManager(fullMode = exportMode == "full")

    # Load all agent definition files in rootAgentFolder
    agentDefinitionFiles = []
    # Load all available Input-Data into relevant classes 
    #ConvertRows to RowClassInstances to provide an easy way of processing and manipulating input data
    if os.path.exists(os.path.join(rootFolderWithAgentDefinitions,"agentDefinition.csv")):
        agentDefinitionFiles.append(os.path.join(rootFolderWithAgentDefinitions,"agentDefinition.csv"))

    for folder in [f.path for f in os.scandir(rootFolderWithAgentDefinitions) if f.is_dir()]:
        if os.path.exists(os.path.join(folder,"agentDefinition.csv")):
            agentDefinitionFiles.append(os.path.join(folder,"agentDefinition.csv"))

    agentAssetList = []
    for agentDefinitionFile in agentDefinitionFiles:

        agentDefinitionCsvContentAsDict = helpers.readCsvIntoDict(agentDefinitionFile)

        agentDefinitionRowsAsClasses =  convertRowsToClasses(agentDefinitionCsvContentAsDict)

        #Enrich agent data from rows with information related to Agent-Defintions with the various Datasources
        newAgentAssetList, agentErrorList = extractAgentDefinitions(agentDefinitionRowsAsClasses, mindsphereDataModelManager)
        for agentAsset in newAgentAssetList:
            rootPathOfDefinitionFile = os.path.dirname(agentDefinitionFile)
            agentAsset.agentData.rootPathOfDefinitionFile = rootPathOfDefinitionFile
            print(rootPathOfDefinitionFile)
            for dataSource in agentAsset.agentData.dataSources:
                if not os.path.isfile(dataSource.dataPointsFileName): # In case the path is no absolut path
                    dataSource.dataPointsFileName = os.path.join(rootPathOfDefinitionFile, dataSource.dataPointsFileName)
                dataPointcsvContentAsDict = helpers.readCsvIntoDict(dataSource.dataPointsFileName)
                dataPointRowsAsClasses =  convertRowsToClasses(dataPointcsvContentAsDict, mode = "dataPointMappings")
                extractAndAttachDataPointDefinitions(agentAsset, dataSource, dataPointRowsAsClasses, mindsphereDataModelManager)

        agentAssetList.extend(newAgentAssetList)



    #pdb.set_trace()
    targetAssets, mappingTable  = createVirtualTargetAssetsFromDict(mindsphereDataModelManager)
    #for each datpoint there is now a mapping mode available, and a potential target asset definition and targetAssetType Name

    # 4. Go through all agents and evaluate the stages of the mapping mode    
    agentAssetsToBeCreatedList = []
    targetAssetsToBeCreatedList = []
    assetTypeList = []

    for agentAsset in agentAssetList:
        print(agentAsset.agentData.mappingMode)
        if "CreateAgent" in agentAsset.agentData.mappingMode: #TODO: Include / Test / Make it clear, that "create" also means Updating Agents (datasources)
            agentAssetsToBeCreatedList.append(agentAsset) 
        # Iterate through all datapoints and build up aspect and assetType definitions, if they are needed, and also fill the target asset list

        # TODO SHIT IS SHIT
        for dataSource in agentAsset.agentData.dataSources:
            for dataPoint in dataSource.dataPoints:
                for dataPointMapping in dataPoint.dataPointMappings:
                    targetAsset = mappingTable[dataPointMapping.targetAsset]
                    dataPointMapping.targetAsset = targetAsset
                    if "DeriveAsset" in dataPointMapping.mappingMode and targetAsset.requiredToBeImported:
                        targetAssetsToBeCreatedList.append(targetAsset)
                    if "DeriveType" in dataPointMapping.mappingMode:
                        if targetAsset.referenceToAssetTypeObject not in assetTypeList:
                            assetTypeList.append(targetAsset.referenceToAssetTypeObject)


    # A) Create all agents, if this is configured
    createAndOnboardAgents(agentAssetsToBeCreatedList)

    # B) Derive all AssetTypes, if this is configured
    importAssetTypesWithoutCheckingMuch(assetTypeList,mindsphereDataModelManager)
    # C) Create all target assets 
    importAssetsWithoutCheckingAnything(targetAssetsToBeCreatedList,mindsphereDataModelManager)
     #addAssetId from import to the agent object, so that mapping can be done
    
    # D) Do the mapping here

    for agentAsset in agentAssetList:
        # Iterate through all datapoints and build up aspect and assetType definitions, if they are needed, and also fill the target asset list
        for dataSource in agentAsset.agentData.dataSources:
            for dataPoint in dataSource.dataPoints:
                for dataPointMapping in dataPoint.dataPointMappings:
                    if "Map" in dataPointMapping.mappingMode:
                        createDatapointMapping(dataPointMapping, agentAsset)

        #Apply Configuration for each agent
        
        if agentAsset.agentData.deviceConfiguration.boardingStatus and agentAsset.agentData.deviceConfiguration.boardingStatus.lower() == "onboarded":
            #Applying changes to device will only work if the device is already onboarded
            applyMappingConfigurationToDevice(agentAsset)
       