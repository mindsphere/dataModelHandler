import requests
import json
import datetime, traceback
import os
import sys
import configparser
import traceback
import csv
from pprint import pprint


#Custom Modules
import helpers
from helpers import ContinueHelper
import modules.readConfig as config
from modules.helpers import ToolMode
from modules.helpers import SimpleError
from modules.mindsphereDataModelManager import MindsphereDataModelManager
from modules.mindsphereApiCollection import offBoardAgent, deleteAsset, deleteAssetType, deleteAspect
from modules.datamodelClassDefinitions import Asset, AssetType, Aspect, ImportStatus
from modules.rowProcessor import convertRowsToClasses, extractAssetDefinitions, extractAspectDefinitions, extractAssetTypeDefinitions, extractDataFromDeletionInputList

############ 
# Potential Bug: Deletion Engine's "Get Agent" call does not work using the session token.
#  This seems to need to be running in another context URL ([tenant]-assetmanger gives insufficent scope error

#######################################
########### MODULE CONFIG #############
#######################################
thisModule = sys.modules[__name__]
requiredParamters= "logging, tenantname, assetDeletionInputFile"
config.setSimpleConfigParametersToModule(thisModule, requiredParamters)


#######################################
############ MAIN BLOCK ###############
#######################################




def flagErrorAssetTypes(assetTypeList, assetsWithErrorList):
    for assetType in assetTypeList:
        for assetId in assetType.relatedToAssetIds:
            assetWithError = next((x for x in assetsWithErrorList if x.assetId == assetId), None)
            if assetWithError:
                assetType.error.addHardError("Deletion of AssetType '{}' won't be tried, since this assetType is related to asset '{}' which already failed to be deleted".format(assetType.id, assetWithError.name))
                break

#######################################

def flagErroraspects(aspectList, assetTypesWithErrorList):
    for aspect in aspectList:
        for aspectId in aspect.relatedToAssetTypeIds:
            aspectWithError = next((x for x in assetTypesWithErrorList if x.id == aspectId), None)
            if aspectWithError:
                aspect.error.addHardError("Deletion of Aspect won't be tried, since this Aspect is related to AssetType '{}' which already failed to be deleted".format(aspectWithError.name))
                break

#######################################

def populateErrorToAllParents(assetWithError, allAssets, errorMessage):
        currentParentAsset = next((x for x in allAssets if x.id == assetWithError.parentId), None)
        if currentParentAsset:
            errorMessage = "ParentAsset '{}' is also affected through this underlying error: ".format(currentParentAsset.name) + errorMessage
            
            currentParentAsset.error.addHardError(errorMessage)
            populateErrorToAllParents(currentParentAsset, allAssets, errorMessage)

#######################################

def evaluateUserInputRegardingOffboarding(assetsWithChilds):

    assetsToBeOffboarded = []
    criticalAssets = [x for x in assetsWithChilds if x.agentData and not x.offboardAgents]

    if criticalAssets:
        print("The following assets cannot be deleted, because they are agents and the input data does not allow offboarding of agents")
        [print(asset.name) for asset in criticalAssets] 

        if input ("\nWould you like to abort the deletion process and rather adjust your input data? (y)\n").lower() == "y":
            sys.exit(0)

        if input ("\nAre you shure you want to go on instead of fixing your inconsistent input? (y/n)\n").lower() != "y":
            sys.exit(0)
        for asset in criticalAssets:
            errorMessage = "Asset '{}' is an agent asset which is configured to not being offboarded -> Deletion won't take place".format(asset.name)
            asset.error.addHardError(errorMessage)
            populateErrorToAllParents(asset, assetsWithChilds, errorMessage)


    assetsToBeOffboarded = [x for x in assetsWithChilds if x.agentData and x.offboardAgents]

    if assetsToBeOffboarded:
        print("The following agents are going to be offboarded!")   
        [print(asset.name) for asset in assetsToBeOffboarded] 

        if input ("\nDo you really want to continue and offboard those agents? (n) \n").lower() != "y":
                sys.exit(0)
        if input ("\nReally? (n) \n").lower() != "y":
                sys.exit(0)
    
    return assetsToBeOffboarded

#######################################

def offboardAgents(assetsToBeOffboarded):
    for asset in assetsToBeOffboarded:
        result = offBoardAgent(asset)
        print(result)

    # in case offboarding fails recursivley flag all related parent assets as failed


#######################################


def start():

    """  
        + -> Get MindSphere Datamodell
        + -> Collect all given assetnames from input file via ID or name
        + -> provide warning if no ID is given an assetname is not unique -> in this case advise the user to provide internal asset-ID and abort run
        + -> if assetname is unique derive ID of asset
        + -> Collect all child assets for all assets 
        + -> rearange list with childs first
        + -> if "datamodel deletion" is activated for such an asset, derrive all assetTypes and add them to a list (including the datamodel-deletion flag)
        + -> add other assetTypes from input file to this assetTypeList
        + -> show all assets to be deleted and ask user to proceed: 
        + -> delete all assets from list -> flag failed assets and flag related assetTypes
        + -> now process assetType list: 
        -> if "datamodel deletion" is activated for such an assetType, derrive all aspects and add them to a list
        + -> add other aspects from input file aspectList
        + -> show all assetTypes to be deleted and ask user to proceed: 
        -> delete all assetTypes from assetTypeList -> flag failed assetTypes and flag related aspects
        -> show all Aspects to be deleted and ask user to proceed
        -> delete all aspects from aspectList
    """

    ##############
    # 1. Extract all assets, asset-types and aspects from Mindsphere
    print("="*80)
    print("Extracting current MindSphere data model now...")
    
    #fullMode braucht man nicht unbedingt - ggfs reichen die Assets alleine aus
    mindsphereDataModelManager =  MindsphereDataModelManager(fullMode = True)

    ##############
    # 2. Load all available Data into relevant classes - multiple assignments are possible
    #ConvertRows to RowClassInstances to provide an easy way of processing and manipulating input data 

    csvContentAsDict = helpers.readCsvIntoDict(assetDeletionInputFile)
    rowsAsClasses =  convertRowsToClasses(csvContentAsDict)

    ####################### ASSETS #######################

    ##############
    # 3 Collect all given assetnames from input file via ID or name
    print("="*80)
    print("Reading data from input list and looking it up within MindSphere ...")
    initialAssetList, initialAssetTypeList, initialAspectList, errorAssetList , errorAssetTypeList , errorAspectList = \
        extractDataFromDeletionInputList(rowsAsClasses, mindsphereDataModelManager)


    ##############
    # 4 Collect all child assets for all identified assets
    print("="*80)
    print("Recursive lookup and aggregation of children of assets ...")
    assetsWithChilds = []
    for currentAsset in initialAssetList:
        assetsWithChilds.extend(mindsphereDataModelManager.getListWithAllChildren(currentAsset, parentsFirst=False))
    
    for element in assetsWithChilds:
        print(element.name)

    #remove potential Duplicates, but keep order 
    # the order is relevant, that the children are first in the list (since they have to be deleted first)
    seen = set()
    seen_add = seen.add
    assetsWithChilds = [x for x in assetsWithChilds if not (x in seen or seen_add(x))]

    for element in assetsWithChilds:
        print(element.name)
    #[asset.simpleOutput() for asset in assetsWithChilds] #TODO Remove, this is only for Debugging
    print("="*80)

    if logging == "VERBOSE": #TODO Remove, this is only for Debugging
        print("-"*20 + " ASSETS " + "-"*20)

        [asset.output() for asset in assetsWithChilds]
        print("-"*20 + " ASSET TYPES " + "-"*20)
        [assetType.output() for assetType in initialAssetTypeList]
        print("-"*20 + " ASPECTS " + "-"*20)
        [aspect.output() for aspect in initialAspectList]
        print("-"*20 + " ASSETS WITH ERROR " + "-"*20)
        [asset.output() for asset  in errorAssetList]
        print("-"*20 + " ASSET TYPES WITH ERROR " + "-"*20)
        [assetType.output() for assetType in errorAssetTypeList]
        print("-"*20 + " ASPECTS WITH ERROR " + "-"*20)
        [aspect.output() for aspect in errorAspectList]   


    ##############
    # 5 Find and add all related assetTypes for the identified assets
    assetTypeList = initialAssetTypeList
    for asset in assetsWithChilds:

        if not asset.typeId.startswith("core"):


            currentAssetType = mindsphereDataModelManager.getAssetType(asset.typeId)
            
            if asset.deleteUnderlyingDatamodel: #Populate this information to assetType objects, too 
                currentAssetType.deleteUnderlyingDatamodel = True 
                currentAssetType.relatedToAssetIds.append(asset.assetId) #Remeber all assets, that are using this assetType - if one of those assets fail to be deleted, assetTypes deletion does not need to be tried
                if not currentAssetType.id.startswith("core"):
                    assetTypeAlreadyInList = next((x for x in assetTypeList if x.id == currentAssetType.id), None) 
                    if not assetTypeAlreadyInList:
                        assetTypeList.append(currentAssetType)


    ##############
    # 7 Show agents to user and ask for confirmation to continue offboarding
    assetsToBeOffboarded = evaluateUserInputRegardingOffboarding(assetsWithChilds)


    ##############
    # 8 Offboard all eligible agents - in case offboarding fails recursivley flag all related parent assets as failed
    offboardAgents(assetsToBeOffboarded)
      

    ##############
    # 9. Print current state and tell user, what assets wil be deleted
    print("="*80)
    assetsWithError = [x for x in assetsWithChilds if x.error.hardError]
    if assetsWithError:
        print("Assets with error that won't be deleted:")
        for asset in assetsWithError:
            asset.output()

        if input ("\nSince there are errors with some assets (even though the deletion process has not even started yet)...\n" + \
            "... do you rather want to abort this job and adjust your configuration first (y)?\n").lower() == "y":
            sys.exit(0)
        print("Well...up to you ...")
    
    flagErrorAssetTypes(assetTypeList,assetsWithError)

    print("="*80)

    assetsToBeProcessed = [x for x in assetsWithChilds if not x.error.hardError]
    print("Assets to be deleted in this job:")
    if assetsToBeProcessed:
        for asset in assetsToBeProcessed:
            asset.output()
        if input ("\nDo you want to continue with the deletion of those assets?(y/n)\n").lower() != "y":
            sys.exit(0)
    else:
        print("No Assets will be deleted ")
    print("="*80)


    ##############
    # 11. Iterate through all Assets from the list and delete them
    print("="*80)
    if assetsToBeProcessed:
        print("Starting asset deletion process now - hopefully you thought through what you are doing here...")
        for asset in assetsToBeProcessed:
            returnDict = deleteAsset(asset)

            statusCode = int(returnDict["statusCode"])
            response = returnDict["response"]
            responseText = returnDict["responseText"]
            
            if statusCode > 200 and statusCode <300:
                asset.setAssetDeletionSucceeded(returnDict)

            else:
                asset.error.addHardError("Asset Deletion failed, response text was: {}".format(responseText))
                asset.setAssetDeletionFailed(returnDict)

    assetsWithFailedDelete = [x for x in assetsToBeProcessed if x.error.hardError]

    if assetsWithFailedDelete:
        print("The following asset deletions failed:\n")   
        counter = 0 
        for asset in assetsWithFailedDelete:
            asset.simpleOutput()
            counter += 1
            print("HARD ERROR: {}, Message: {}".format(asset.error.errorstatus, str(asset.error.errortext)))
    else:
        print("No failed asset deletions, which is nice.")
    ################### ASSET TYPES #####################

    ##############

    # 12. Derive Aspects from AssetTypes and add them to aspect list 

    assetTypesWithErrorAlreadyAtBeginning = [x for x in assetTypeList if x.error.hardError]
    assetTypesToBeProcessed = [x for x in assetTypeList if not x.error.hardError]

    flagErrorAssetTypes(assetTypesToBeProcessed,assetsWithFailedDelete)

    aspectList = initialAspectList
    for assetType in assetTypesToBeProcessed:
        if assetType.deleteUnderlyingDatamodel: # Do aspects need to be deleted, too
            assetTypeDefinitionFromMindsphere = mindsphereDataModelManager.getAssetType(assetType.id)
            #TODO REWORK
            for aspect in assetTypeDefinitionFromMindsphere.aspects:
                aspectFromMindsphere = mindsphereDataModelManager.getAspect(aspect.id)
                if not aspectFromMindsphere.id.startswith("core"): #Skip core aspects
                    aspectAlreadyInList = next((x for x in aspectList if x.id == aspectFromMindsphere.id), None) 
                    if not aspectAlreadyInList: #Only add aspects to list, if they are not already in it
                        aspectList.append(aspectFromMindsphere)
                    else:
                        aspectAlreadyInList.relatedToAssetTypeIds.append(assetType.id)



    ##############
    # 13. Print current state regarding assetTypes and tell user, which will be deleted
    print("="*80)

    if assetTypesWithErrorAlreadyAtBeginning:
        print("AssetTypes with input error (no deletion will be attempted for those assetTypes):")
        for assetType in assetTypesWithErrorAlreadyAtBeginning:
            assetType.output()    

    print("="*80)

    assetTypesWithErrorAfterAssetDeletion = [x for x in assetTypesToBeProcessed if x.error.hardError]
    assetTypesToBeProcessed = [x for x in assetTypesToBeProcessed if not x.error.hardError]

    if assetTypesWithErrorAfterAssetDeletion:
        print("AssetTypes where an error occured when trying to importing related assets (no deletion will be attempted for those assetTypes anymore):")
        for assetType in assetTypesWithErrorAfterAssetDeletion:
            assetType.output()    

    print("="*80)

    print("AssetTypes to be deleted in this job:")
    if assetTypesToBeProcessed:
        for assetType in assetTypesToBeProcessed:
            assetType.output()
        if input ("\nDo you want to continue with the deletion of those assetTypes?(y/n)\n").lower() != "y":
            sys.exit(0)
    else:
        print("No AssetTypes will be deleted ")

    print("="*80)


    ##############
    # 15. Iterate through all AssetTypes from the list and delete them
    print("="*80)
    assetsTypesWithFailedDelete = []
    if assetTypesToBeProcessed:
        print("Starting AssetType deletion process now ...")
        for assetType in assetTypesToBeProcessed:
            returnDict = deleteAssetType(assetType)

            statusCode = int(returnDict["statusCode"])
            response = returnDict["response"]
            responseText = returnDict["responseText"]
            
            if statusCode > 200 and statusCode <300:
                assetType.setAssetTypeDeletionSucceeded(returnDict)

            else:
                assetType.error.addHardError("AssetType Deletion failed, response text was: {}".format(responseText))
                assetType.setAssetTypeDeletionFailed(returnDict)

    
        assetsTypesWithFailedDelete = [x for x in assetTypesToBeProcessed if x.error.hardError]
        if assetsTypesWithFailedDelete:
            print("The following Asset-Type deletions failed:\n")   
            counter = 0 
            for assetType in assetsWithFailedDelete:
                assetType.simpleOutput()
                counter += 1
                print("HARD ERROR: {}, Message: {}".format(asset.error.errorstatus, str(asset.error.errortext)))
        else:
            print("No failed AssetType Deletions, which is nice.")


    ##############
    # 16. Print current state regarding aspects and tell user, which will be deleted
    print("="*80)

    # First flag all aspects with information from failed deletions on higher level datamodel layers (= failures with deletion of assets or aspects)
    flagErroraspects(aspectList, assetTypesWithErrorAlreadyAtBeginning)
    flagErroraspects(aspectList, assetTypesWithErrorAfterAssetDeletion)
    if assetsTypesWithFailedDelete:
        flagErroraspects(aspectList, assetsTypesWithFailedDelete) 

    aspectsWithError = [x for x in aspectList if x.error.hardError]
    aspectsToBeProcessed = [x for x in aspectList if not x.error.hardError]

    if aspectsWithError:
        print("Aspects with error that won't be deleted:")
        for aspect in aspectsWithError:
            aspect.output()    

    print("="*80)

    print("Aspects to be deleted in this job:")
    if aspectsToBeProcessed:
        for aspect in aspectsToBeProcessed:
            aspect.output()
        if input ("\nDo you want to continue with the deletion of those Aspects?(y/n)\n").lower() != "y":
            sys.exit(0)
    else:
        print("No Aspects will be deleted ")

    print("="*80)


    ##############
    # 18. Iterate through all AssetTypes from the list and delete them
    print("="*80)
    if aspectsToBeProcessed:
        print("Starting Aspect deletion process now ...")
        for aspect in aspectsToBeProcessed:
            returnDict = deleteAspect(aspect)

            statusCode = int(returnDict["statusCode"])
            response = returnDict["response"]
            responseText = returnDict["responseText"]
            
            if statusCode > 200 and statusCode <300:
                aspect.setAspectDeletionSucceeded(returnDict)

            else:
                aspect.error.addHardError("AssetType Deletion failed, response text was: {}".format(responseText))
                aspect.setAspectDeletionFailed(returnDict)

    aspectsTypesWithFailedDelete = [x for x in aspectsToBeProcessed if x.error.hardError]
    if aspectsTypesWithFailedDelete:
        print("The following Aspect deletions failed:\n")   
        counter = 0 
        for aspect in aspectsTypesWithFailedDelete:
            aspect.simpleOutput()
            counter += 1
            print("HARD ERROR: {}, Message: {}".format(aspect.error.errorstatus, str(aspect.error.errortext)))
    else:
        print("No failed Aspect Deletions, which is nice.")