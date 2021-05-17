import uuid
import requests
import json
import datetime, traceback
import os
import sys
import configparser
import traceback
import csv
from pprint import pprint
import collections

from pathlib import Path
import helpers


#Custom Modules
import modules.readConfig as config
from modules.helpers import SimpleError, SingleAssetImporterInputDataset, SingleAgentInputDataset, SingleDatapointInputDataset

from modules.mindsphereApiCollection import *
from modules.datamodelClassDefinitions import Asset, AssetType, Aspect, ImportStatus
from modules.rowProcessor import convertRowsToClasses, extractAssetDefinitions, extractAspectDefinitions, extractAssetTypeDefinitions
from modules.mindsphereDataModelManager import MindsphereDataModelManager
from modules.apiHandler import ScopeNotFound
#######################################
########### MODULE CONFIG #############
#######################################

# The following block loads parameters from the config an provides them in an easy to use way in this modul:
# Instead of config.<parametername> you can just use <parametername> as standalone afterwards

thisModule = sys.modules[__name__]
requiredParamters= "logging, tenantname, parentIdsToBeExported, exportMode, exportedDataOutputFile, exportAgentAndMappingConfiguration, convertToNeutralIds"
config.setSimpleConfigParametersToModule(thisModule, requiredParamters)

#Create Directory for Output#

agentDefinitionsFileName = "agentDefinition.csv"
directory = os.path.dirname(exportedDataOutputFile)
Path(directory).mkdir(parents=True, exist_ok=True)

#######################################
######### HELPER FUNCTIONS ############
#######################################




def convertMappingDictToOrderedColumnDict(mappingDict):
    outputColumnsList = []
    for attribute in mappingDict:

        if isinstance(mappingDict[attribute], dict):
            columnheaders = mappingDict[attribute]["matchingInputHeaders"]
            if mappingDict[attribute].get("excludeFromExport"):
                continue
            
        else:
            columnheaders = mappingDict[attribute]
        
        if isinstance(columnheaders, tuple): 
            value = columnheaders[0] #In case there are multiple allowed headers for an output file, take the first one
        else:
            value = columnheaders
    
        outputColumnsList.append(value)

    return collections.OrderedDict([ (k, None) for k in outputColumnsList])


#######################################

#######################################
############ MAIN BLOCK ###############
#######################################

def attachDataSourcesToAgentAsset(agentAsset, mindsphereDataModelManager):
    mindsphereDataModelManager.collectDataSourcesForAgent(agentAsset)
   
#######################################

def attachDeviceConfigToAgentAsset(agentAsset, mindsphereDataModelManager):
    mindsphereDataModelManager.collectDeviceConfigForAgent(agentAsset)

#######################################

def attachDatapointMappingsToAgentAsset(agentAsset,listOfAllAssetsToBeProcessed, mindsphereDataModelManager):
    omittedTargetAssets = []
    mindsphereDataModelManager.collectValidDataPointMappingForAgent(agentAsset)
    for dataSource in agentAsset.agentData.dataSources:
        for dataPoint in dataSource.dataPoints:
            for mapping in dataPoint.dataPointMappings:
                if mapping.targetAsset:
                    targetAssetInScope = next((asset for asset in listOfAllAssetsToBeProcessed if asset.assetId == mapping.targetAsset.assetId),None)
                    if not targetAssetInScope:
                        if not next((asset for asset in omittedTargetAssets if asset.assetId == mapping.targetAsset.assetId), None):
                            omittedTargetAssets.append(mapping.targetAsset)
                            print(f"Attention! A target Device-Asset '{mapping.targetAsset.name}' for a mapping is not included in export defintion. Datapoint Mappings pointing to this asset will be omitted from export!")
                            #TODO Review: Das Ding trotzdem zu exportieren, mit einer Referenz auf ein Dummy Asset macht vermutlich keinen Sinn

            
               
                else:
                    print(f"Attention! A target Device-Asset for a mapping is not existing in MindSphere.")
                    print(f"The related agent is: '{agentAsset.name}'). This is probably due to an invalid mapping ...")

            dataPoint.dataPointMappings = list(filter(lambda x: mapping.targetAsset not in omittedTargetAssets, dataPoint.dataPointMappings))


#######################################

def addFullAgentInformation(assetsToBeProcessed, mindsphereDataModelManager):
    
    for asset in assetsToBeProcessed:
           if asset.agentData:
            try:
                # First, get a potential device config
                attachDeviceConfigToAgentAsset(asset, mindsphereDataModelManager)
                
                attachDataSourcesToAgentAsset(asset, mindsphereDataModelManager)

                if asset.agentData.dataSources:
                    # Last step: add all the existing mappings, but only if the mapped asset is part of the export
                    attachDatapointMappingsToAgentAsset(asset, assetsToBeProcessed, mindsphereDataModelManager)
             

            except ScopeNotFound as e:
                print(f"!!! Attention !!! Skipping collecting of agent information for the asset '{asset.name}'...")
                print(f"    Reason: Unkown assetType '{e}' for agent-scope detection!")
                print("")
                

    return None

#######################################
     
def start():



    # 1. Extract all assets (and optionally asset-types and aspects) from Mindsphere
    print("="*80)
    print("Initialize MindSphere data model now...")
    
    mindsphereDataModelManager = MindsphereDataModelManager(fullMode = exportMode == "full")

    # 2. Find all children
    # Go through list of parents to be exported and add all childs-assets to an export batch
    exportBatches = {}

    #In case there is only one parent asset to be exported, convert it to a list first
    parentIdsToBeExportedAsList = []
    if isinstance(parentIdsToBeExported, str): 
        parentIdsToBeExportedAsList.append(parentIdsToBeExported)
    else:
        parentIdsToBeExportedAsList = parentIdsToBeExported
    print("")
    print("... identifing data to be exported based on the source definition for the export now")

    for parentID in parentIdsToBeExportedAsList:
        currentParentAssets = mindsphereDataModelManager.getAssetsIncludingUserDecision(parentID, allowMultipleSelections=True)       
        if not currentParentAssets:
            continue

        for assetToBeExported in currentParentAssets:
                assetsWithChilds = mindsphereDataModelManager.getListWithAllChildren(assetToBeExported, parentsFirst=True) 
                exportBatches[assetToBeExported.assetId] = assetsWithChilds


    # 3. Merge all export batches for all given parents
    # Now merge data for all given export batches (which might have been definied via various parent ids given in the configuration file)
    mergedAssetListForExport = []

    for currentBatch in exportBatches:
        for currentAsset in exportBatches[currentBatch]:
            if not next((x for x in mergedAssetListForExport if currentAsset.assetId == x.assetId), None):
                mergedAssetListForExport.append(currentAsset)


    # 4. Add Agent Data 
    # This only takes place in case datasources and mapping should also be exported (which is defined in the config):
    if exportAgentAndMappingConfiguration:
        # Go through list with all agents and mark the coressponding assets as agents:
        # If asset is an agent, add the agent data with device information, defined datasources and mappings

        # For the ID mapping use a tracking dict, that maps internal IDs to neutral IDs (agentIDs, DatasourceIDs, Mapping IDs)
        # If an agent AgentAsset has been identified, the mapping list has to be looked through, to identify mapped assets:
        # -> In case the mapped assets are part of the export, they also need a neutral flag.
        # -> In case the are not included in the export, a warning should be provided, but the asset will not be exported
        print("... collecting agent data for assets to be exported now")
        addFullAgentInformation(mergedAssetListForExport, mindsphereDataModelManager)

    # 5. Derive lists with columns that need to be populated during the export run and that will be existing in the output format
    # Those columns should be the "opposite" of the import attributes, so that the import and export formats are compatible

    # Preparation for assets

    # The mapping dictionary contains the relation between object-attributes and the columns in the output-file
    mappingDictAssets = SingleAssetImporterInputDataset.mappingDict

    orderedOutputColumnsDictForAssetExport = convertMappingDictToOrderedColumnDict(mappingDictAssets)

    # 6. Export asset information as csv
    outputListAssets = []
    # Initialize an empty ordered dict that will be populated during the export preparation
    # The keys are the names of the output

    for currentAsset in mergedAssetListForExport:
        print("Processing asset '{}' with id '{}' now".format(currentAsset.name, currentAsset.assetId))
        workingDictList = currentAsset.outputAsDictionaryList(orderedOutputColumnsDictForAssetExport, fullMode = exportMode == "full")
        outputListAssets.extend(workingDictList)
    
    # 7 Write asset Information to csv - this could be an issue since it overwrites everything with that tenantname (also descriptions and such)
    replaceList = [{"oldString": tenantname + ".", "newString" : "[$$$TENANTNAME$$$]."}]
    helpers.writeListOfSimilarDictToCsv(outputListAssets, exportedDataOutputFile, replaceList)


    # 7. Prepare and export relevant agent information as csv
    if exportAgentAndMappingConfiguration:
        # Preparation for agents
        mappingDictAgents = SingleAgentInputDataset.mappingDict
        mappingDictDatapoints = SingleDatapointInputDataset.mappingDict

        orderedOutputColumnDictForAgents = convertMappingDictToOrderedColumnDict(mappingDictAgents)
        orderedOutputColumnDictForDatapoints = convertMappingDictToOrderedColumnDict(mappingDictDatapoints)

        for currentAsset in mergedAssetListForExport:
            if currentAsset.agentData:
                #Create Subfolder for all Agent related data
                agentDirectory = os.path.join(directory,"agentDefinitions",currentAsset.name + "_" + currentAsset.assetId)
                Path(agentDirectory).mkdir(parents=True, exist_ok=True)

                #Export Agent-Data with Datasource-Definitions
                agentDataDictList = currentAsset.agentData.getDictListOutputForAgent(orderedOutputColumnDictForAgents)
                replaceList = []
                agentDefinitionFileName = os.path.join(agentDirectory,agentDefinitionsFileName)
                helpers.writeListOfSimilarDictToCsv(agentDataDictList, agentDefinitionFileName, replaceList)
                
                #Export DataPoints with DataPointMappings for each Datasource,
                for dataSource in currentAsset.agentData.dataSources: 

                    agentDatapointsAndMappingsDictList = dataSource.getDictListOutputForDataPointsAndMappings(orderedOutputColumnDictForDatapoints)
                    dataSourceFileName = os.path.join(agentDirectory,dataSource.dataPointsFileName)
                    helpers.writeListOfSimilarDictToCsv(agentDatapointsAndMappingsDictList, dataSourceFileName, replaceList)
                
                
    #Print out list of all neutral IDs
    if logging in ("VERBOSE"): 
        lookUpNeutralId()
  
