import requests
import uuid
import json
import datetime, traceback
import os
import sys
import configparser
import traceback
import csv
import pprint
import helpers
from helpers import ContinueHelper
import re
import pdb
import modules.readConfig as config

#Custom Modules
import helpers
from modules.helpers import ToolMode
from modules.helpers import SimpleError
from modules.mindsphereDataModelManager import MindsphereDataModelManager
from modules.mindsphereApiCollection import createNewAssetInMindSphere, createNewAspectInMindSphere, createNewAssetTypeInMindSphere
from modules.datamodelClassDefinitions import Asset, AssetType, Aspect, ImportStatus
from modules.rowProcessor import convertRowsToClasses, extractAssetDefinitions, extractAspectDefinitions, extractAssetTypeDefinitions
from modules.timeseriesSimulator import getSimulatedData

#######################################
########### MODULE CONFIG #############
#######################################

thisModule = sys.modules[__name__]
requiredParamters= "logging, tenantname, defaultParentAssetName, defaultParentAssetId, assetImportInputFile"
config.setSimpleConfigParametersToModule(thisModule, requiredParamters)


#######################################
############ MAIN BLOCK ###############
#######################################


def importAssetStructureToMindSphere(assetsToBeImported):

    successedAssets = []
    successedAssetTypes= []
    successedAspects = []
    failedAssets = []
    failedAssetTypes= []
    failedAspects =[]
    
    for asset in assetsToBeImported:
        
        if asset.error.hardError == True or asset.alreadyExistingInMindSphere: # asset could have already been imported before
            continue #Skip Items with error and Items alread in MindSphere
        if asset.parentId == None: #Now check, if parentName has already been imported during the current import process (check if parentName is in "successfully imported list")

            parent = next((x for x in successedAssets if x.name == asset.parentAssetNameOrId and x.neutralAssetId == asset.neutralParentId), None)
            if parent and parent.name:
                asset.parentId = parent.assetId
            else: #No parentName available - import will definitely fail
                asset.error.addHardError(f"Import of Asset '{asset.name}' cannot be performed because it's parent seems to be missing")
                asset.assetImportStatus = ImportStatus.FAILED #Set general Asset-Status to failed
                
        
        if not asset.suitableAssetTypeExistingInMindSphere: #now check if there has already been a suitable assetType being imported
            
            for aspect in asset.assetTypeToBeImported.getAspectsThatNeedImporting(): 
                
                if asset.assetImportStatus != ImportStatus.FAILED:
                #Only go on, if nothing has already failed on this asset
                    aspectAlreadyImported = next((x for x in successedAspects if x.id == aspect.id), None)

                    if aspectAlreadyImported:
                        aspect.AspectImportStatus = ImportStatus.NOT_NECESSARY
                        #No need to import this aspect, someone else did it before.
                        #Continute with next aspect
                        continue

                    aspectAlreadyFailed = next((x for x in failedAspects if x.id == aspect.id), None)
                    if aspectAlreadyFailed:
                        #Abort this asset, since the aspect creation failed already before
                        asset.error.addHardError("Import of Aspect '{}' failed already in a previous atempt, asset wont be imported".format(aspect))
                        asset.assetImportStatus = ImportStatus.FAILED #Set general Asset-Status to failed
                    
                    else: #No failures so far ...
                        # Ok, try to import the aspect
                        returnDict = createNewAspectInMindSphere(aspect)
                        statusCode = int(returnDict["statusCode"])
                        response = returnDict["response"]
                        responseText = returnDict["responseText"]

                        if statusCode >= 200 and statusCode <300:
                            successedAspects.append(aspect)
                            aspect.setAspectImportSucceeded(returnDict)
                        else:
                            asset.error.addHardError("Aspect Import failed, response text was: {}".format(responseText))
                            failedAspects.append(aspect)
                            aspect.setAspectImportFailed(returnDict)
                            asset.assetImportStatus = ImportStatus.FAILED #Set general Asset-Status to failed

            if asset.assetImportStatus == ImportStatus.FAILED:
                #Don't continue import of this asset, something failed before
                continue 

            #Now all requirements for creation of AssetType are given:

            # Import ASSET TYPE HERE
            assetTypeAlreadyImported = next((x for x in successedAssetTypes if x.id == asset.assetTypeToBeImported.id), None) # and x["parentTypeId"] == asset.assetTypeToBeImported.parentAssetTypeId)  -> this additional check is not useful, since aspectNames are unique in MindSphere
            if assetTypeAlreadyImported:
                asset.assetTypeToBeImported.assetTypeImportStatus = ImportStatus.NOT_NECESSARY

            else:
                #Only Import something, if it has not already been tried to import it before ....

                assetTypeAlreadyFailed = next((x for x in failedAssetTypes if x.id == asset.assetTypeToBeImported.id), None)
                if assetTypeAlreadyFailed:
                    #Abort this asset, since the aspect creation failed already before
                    asset.error.addHardError("Import of AssetType '{}' failed already in a previous atempt, asset wont be imported".format(asset.assetTypeToBeImported.id))
                    asset.assetImportStatus = ImportStatus.FAILED #Set general Asset-Status to failed
                    #Continue with next asset
                    continue
            
                #Everthing is looking fine so far, starting importing of assetType now
                
                returnDict = createNewAssetTypeInMindSphere(asset.assetTypeToBeImported)
                statusCode = int(returnDict["statusCode"])
                response = returnDict["response"]
                responseText = returnDict["responseText"]

                if statusCode > 200 and statusCode <300:
                    successedAssetTypes.append(asset.assetTypeToBeImported)
                    asset.assetTypeToBeImported.setAssetTypeImportSucceeded(returnDict)
                else:
                    asset.error.addHardError("AssetType Import failed, response text was: {}".format(responseText))
                    failedAssetTypes.append(asset.assetTypeToBeImported)
                    asset.assetTypeToBeImported.setAssetTypeImportFailed(returnDict)
                    asset.assetImportStatus = ImportStatus.FAILED # Also Set general Asset-Status to failed
            
        if asset.assetImportStatus == ImportStatus.FAILED:
            #Don't continue import of this asset, something failed before
            continue 

        # At this point it should finally possible to import the Asset itself into MindSphere:
        
        returnDict = createNewAssetInMindSphere(asset)
        statusCode = int(returnDict["statusCode"])
        response = returnDict["response"]
        responseText = returnDict["responseText"]

        if statusCode > 200 and statusCode <300:
            successedAssets.append(asset)
            asset.setAssetImportSucceeded(returnDict)

        else:
            asset.error.addHardError("Asset Import failed, response text was: {}".format(responseText))
            failedAssets.append(asset)
            asset.setAssetImportFailed(returnDict)



    print("All asset imports have been completed or at least attempted")

def showInformationAndAskUserBeforeImporting(finalSortedAssetList, errorCounter):
    
    if errorCounter:
        print(f"Attention. There have been {errorCounter} unidentified errors ... maybe you better abort this run!")
        if input ("\nAre you shure you want to import potential b***s*** instead of fixing your input?\n").lower() != "y":
            print("Probably a wise choise ... fix it and come back later....")
            sys.exit(0)
        else:
            print("Up to you...going on then ")   
        
    print("Listing all assets with error and their error status...")
    print("*"*40)
    print("Assets with soft error:")
    [print("Assetname '{}': {}".format(asset.name, str(asset.error.errortext))) for asset in finalSortedAssetList if asset.error.errorstatus == True and asset.error.hardError == False]
    print("*"*40)
    print("Assets with hard error:")
    [print("Assetname '{}': {}".format(asset.name, str(asset.error.errortext))) for asset in finalSortedAssetList if asset.error.hardError == True]
    print("*"*40)
    print("*"*80)
    counter = 0
    print("!!! The following assets will be imported (import will take place in the listed order):\n")
    for asset in finalSortedAssetList:
        if asset.error.hardError == False and not asset.alreadyExistingInMindSphere:
            counter += 1
            print("\n--> AssetName: '{}' {}; AssetType: '{}'; Description: '{}', parentName: '{}' {}".format(asset.name, '(with internal asset Id: ' + asset.neutralAssetId +")" if asset.neutralAssetId else "", asset.typeId, asset.description,asset.parentAssetNameOrId, '(with internal parent Id: ' + asset.neutralParentId  +"')" if asset.neutralParentId else ""))
            assetTypeObject = asset.assetTypeToBeImported  
            if assetTypeObject: 
                print ("     ...requires import of AssetType: '{}'; Description: '{}', Ancestor of Asset-TypeId: '{}' ".format(assetTypeObject.name, assetTypeObject.description, assetTypeObject.ancestorOfTypeId))
                if len(assetTypeObject.getAspectsThatNeedImporting()) > 0:
                    for aspect in assetTypeObject.getAspectsThatNeedImporting():
                        if aspect:
                            print ("          ...requires import of Aspect: '{}'; Description '{}'".format(aspect.name, aspect.description))
                            for variable in aspect.getVariables():
                                print ("               ...with variablename: '{}', datatype '{}', unit: '{}'".format(variable.name, variable.dataType, variable.unit))

    if counter == 0:
        print("ATTENTION: Nothing found to be imported. Exiting now ...")
        print("\n")
        exit(0)

    print("\n")
    print("*"*40)

    if input ("\nWould you like to continue the import (y/n)? \n-> All assets listed with a hard error state will be omitted in the further process!!\n").lower() != "y":
        sys.exit(0)
    if sum(1 for asset in finalSortedAssetList if asset.error.hardError == True):
        if input ("\nAre you shure you want to import potential b***s*** instead of fixing your input?\n").lower() != "y":
            print("Probably a wise choise ... fix it and come back later....")
            sys.exit(0)
        print("... your choice ...")

def evaluateStatusAfterImporting(finalSortedAssetList):

    print("Presenting the import-status after finishing the job now...")
    print("*"*40)
    print("The following assets have been imported successfully:\n")
    counter = 0
    for asset in finalSortedAssetList:
        if asset.assetImportStatus == ImportStatus.SUCCESSFUL:
            asset.simpleOutput()
            counter += 1
    if counter == 0:
        print("!!!  ---> OUCH: Nothing has been imported successfully !!!\n")

    failedAssets = [asset for asset in finalSortedAssetList if asset.assetImportStatus == ImportStatus.FAILED]
   
    if failedAssets:
        print("*"*40)
        print("The following asset imports failed:\n")  
        for asset in failedAssets:
            asset.simpleOutput()
            print("HARD ERROR: {}, Message: {}".format(asset.error.errorstatus, str(asset.error.errortext)))
        print("\n ... I, the importer tool, am deeply sorry for your loss")
    
    elif counter > 0:
        print("No failed imports, which is nice...")

    print("\n")
    print("*"*40)

def createVFCflows(finalSortedAssetList):

    assetTypes = {}
    for asset in finalSortedAssetList:
        if asset.assetImportStatus == ImportStatus.SUCCESSFUL: 
            if not "core." in asset.typeId:
                if asset.typeId not in assetTypes:
                    assetTypes[asset.typeId] = []
                assetTypes[asset.typeId].append(asset)


    for assetType in assetTypes:
        #prepareFLow based on the assetType Structure
        assetList = assetTypes[assetType] 
        assetTypeObject = assetList[0].assetTypeToBeImported
        assetListWithIds = [asset.assetId for asset in assetList]
        aspectList = []
        simulationDataList = []
        for aspect in assetTypeObject.getAspects():
            aspectList.append(aspect.name)
            currentAspectDataDict = {}
            for variable in aspect.getVariables():
                if variable.flowMin and "-" in variable.flowMin:
                    splitted = variable.flowMin.split('-') 
                    merged =''
                    for element in splitted:
                        if "(" in element:
                            merged += "'" + element +"',"
                        else:
                            merged += element +","
                    merged = merged[:-1]
                    merged = "[" + merged + "]" 
                    #merged = "[" + ','.join(splitted) + "]" 
                    currentAspectDataDict[variable.name] = merged
                elif variable.dataType == "BOOLEAN" and variable.flowLikelihood:
                    currentAspectDataDict[variable.name] = f"getTrueBoolean({variable.flowLikelihood})"
                elif variable.dataType == "INT" and variable.flowMin and variable.flowMax:
                    currentAspectDataDict[variable.name] = f"getRandomInt({variable.flowMin},{variable.flowMax})"
                elif variable.dataType == "DOUBLE" and variable.flowMin and variable.flowMax:
                    currentAspectDataDict[variable.name] = f"getRandomDouble({variable.flowMin},{variable.flowMax})"
            simulationDataList.append(currentAspectDataDict)

        from VFCtemplate import template
        renderedTemplate = template.replace("$assetList",pprint.pformat(assetListWithIds,width=1)) 
        renderedTemplate = renderedTemplate.replace("$aspectList",pprint.pformat(aspectList,width=1))
        renderedTemplate = renderedTemplate.replace("$dataList",pprint.pformat(simulationDataList,width=1))
        renderedTemplate = renderedTemplate.replace("\n", "\\n")
        #Now remove quotes around the getRandom-Function to turn it into true javascript
        renderedTemplate = re.sub(r"'(getRandom.*?)'", r'\1', renderedTemplate)
        renderedTemplate = re.sub(r"'(getTrueBoolean.*?)'", r'\1', renderedTemplate)
        renderedTemplate = re.sub(r'"(\[.*?\])"', r'\1', renderedTemplate)

        filename = f"VFC-Simulation_assetType_{assetType}.tpl"
        
        with open(filename, 'w') as vfcFlowSimulationFile:
            vfcFlowSimulationFile.write(renderedTemplate)


def simulateTimeseriesData(asset):
    
    for aspect in asset.getAspects():
        #Simulate the TS data for this aspect
        inputDefinition =        {
           
        }
        
        dataList = getSimulatedData(inputDefinitions, timestampsAsString=True)
        writeTimeSeriesData(assetId,aspectName,dataList)
   




def validateAspectIntegrity(asset,aspectName,variableDefinitionsLocal,variableDefinitionsMindSphere):
    
    for variable in variableDefinitionsLocal["VariableDefinition"]:

        variableInMindSphere = next((x for x in variableDefinitionsMindSphere if x.name == variable), None)
        if not variableInMindSphere:
            asset.error.addHardError("Variable not available: Variable '{}' from Aspect '{}' does not exist in MindSphere, though the Aspect itself exists".format(variable,aspectName))
            return False
        fullLocalVariableDefinition = variableDefinitionsLocal["VariableDefinition"][variable]  
 
        if fullLocalVariableDefinition["Datatype"] != variableInMindSphere.dataType:
            asset.error.addHardError("Mismatch in Variable-Definition: Variable '{}' from Aspect '{}' was defined with Datatype " 
            "'{}' in the importer's input, but is defined with Datatype '{}' within MindSphere".format(variable,aspectName,fullLocalVariableDefinition["Datatype"],variableInMindSphere.dataType))
            return False

        if fullLocalVariableDefinition["Unit"] != variableInMindSphere.unit:
            asset.error.addHardError("Mismatch in Variable-Definition: Variable '{}' from Aspect '{}' was defined with Unit " 
            "'{}' in the importer's input, but is defined with Unit '{}' within MindSphere".format(variable,aspectName,fullLocalVariableDefinition["Unit"],variableInMindSphere.unit))
            return False             

    return True
   
def recursiveParentSorting(sortedObjectsList,restList,recursionDepth = 1):
    


    if logging in ("INFO", "VERBOSE"):

        print("Current recursion depth when sorting parents: {}".format(recursionDepth))
        
    if recursionDepth > 15:
        print("ERROR: Recursion Depth higher than 15 - something seems to be wrong with your input data - probably there is a circular relation between parents and childs")
        return False
    intermediateList = []
    intermediateRest = []

    # for asset in restList:
    #     if asset.parentName in [asset.name for asset in sortedObjectsList]: 
    #         intermediateList.append(asset)
    #     else: 
    #         intermediateRest.append(asset)

    for asset in restList:

        currentParentAsset = next((otherAsset for otherAsset in sortedObjectsList if asset.parentAssetNameOrId == otherAsset.name and asset.neutralParentId == otherAsset.neutralAssetId), None)
        if currentParentAsset:
            intermediateList.append(asset)
        else: 
            intermediateRest.append(asset)

    sortedObjectsList.extend(intermediateList)

    if intermediateRest:
        sortedObjectsList = recursiveParentSorting(sortedObjectsList,intermediateRest, recursionDepth + 1)
    
    return sortedObjectsList

def start():
    
    ##############
    # 1. Load all available Input-Data into relevant classes - multiple assignments are possible
    
    #ConvertRows to RowClassInstances to provide an easy way of processing and manipulating input data 
    csvContentAsDict = helpers.readCsvIntoDict(assetImportInputFile)
    rowsAsClasses =  convertRowsToClasses(csvContentAsDict)

    #TODO Extract data from rows with information related to Aspect-Definitions -> Aspect Class List - 
    aspectList, errorAspectList = extractAspectDefinitions(rowsAsClasses)
    
    #TODO Extract data from rows with information related to AssetType-Definitions -> AssetType Class List
    assetTypeList, errorAssetTypeList = extractAssetTypeDefinitions(rowsAsClasses)

    # Extract data from rows with information related to Asset-Defintions
    assetList, errorAssetList = extractAssetDefinitions(rowsAsClasses)

    print("="*80)
    print("Results of first check...", end=(''))
    if errorAssetList:
        [print("Error in asset {}, errors are: {}".format(asset.name,str(asset.error.errortext))) for asset in errorAssetList]
        print("All erroneous assets identified already at this point of process won't be processed and mentioned any further:")
    else:
        print("no errors")
    print("="*80)

    ##############
    # 2. Start hierarchical parentName sorting for Assets - Goal: The higher a parentName, the more in the upper part "on top" within this list:
    sortedObjectsList = []
    restList = []
    finalSortedAssetList = []
    #Get all objects, that have no parents in the current list
    for asset in assetList:

        if asset.parentAssetNameOrId not in [asset.name for asset in assetList]: 
            sortedObjectsList.append(asset)
        else: 
            restList.append(asset)
    #Now bring all in the right order
    print("="*80)
    print("Recursive sorting of Asset-List depending on parentName hierarchy ...")
    finalSortedAssetList = recursiveParentSorting(sortedObjectsList,restList)
    print("="*80)
    if logging in ("VERBOSE"):
        print("-"*80)
        print("-"*80)
        for asset in finalSortedAssetList:
            asset.output()
        print("-"*80)
        print("-"*80)


    ##############
    # 3. Now extract all assets, asset-types and aspects from Mindsphere
    print("="*80)
    print("Extracting current MindSphere data model ...")

    #fullMode braucht man nicht unbedingt - ggfs reichen die Assets alleine aus
    mindsphereDataModelManager =  MindsphereDataModelManager(fullMode = True)
    
    # Get ParentID of the defined default asset
    defaultParentObject = mindsphereDataModelManager.getAsset(assetId = defaultParentAssetId, name = defaultParentAssetName)

    if not defaultParentObject:
        print ("\nATTENTION: \nThe default parrentAsset '{}' defined in the config.ini has not been found in the specified MindSphere tenant. \nPlease adapt your config accordingly. \nExiting now ...".format(defaultParentAssetName))
        exit(0)
        
    print("="*80)

    # Iterate through the three sets (Aspects, AssetTypes, Assets) and check for inconsistencies regarding existing Mindsphere Data
    # Mark error on each of those classes and additionaly move asset within the static-class lists (toBeImported, failed, importNotNecessary)

    # Aspects -> Easy, because this is independet of anything else
    # AssetTypes -> check if some of the defined Aspects are in the failed list -> if yes, put assettype into failed list
    # Asset -> check if assetTypes are in the failed list -> if yes, put asset into failed list

    #Import Order: 
    # Aspects, AssetTypes, Assets
    # Response jeweils auch mit rausschreiben.
    # Am Ende kann man ganz einfach aus einer Kombination von den successfully imported rückwärts die Dinge wieder löschen -> Assets, AssetTypes, Aspects

    ##############
    # 4. Iterate through all Elements from the final list and flag, if the asset is already existing and if the defined assetType is existing and fitting
    print("="*80)
    print("Comparing data model to be imported with currently existing MindSphere data model...") 
    continueOuterLoop = ContinueHelper()
    errorCounter = 0
    for asset in finalSortedAssetList:
        try:
            # The script will exit with an error here, if the given name in the input list is not unique.
            # Todo: Also implement IDs for ImportMode
            # Todo: Allow multiple import using asset-index in the input file
            
            ## PARENT TYPE RELATED

            #Check if an asset with the name or id is already existing in Mindsphere 
            # If y, let the user decied, if this asset should be treated as "existing". In this case the importer won't import this line
            # If the user choses None, then a new one will be imported

            assetInMindSphere = mindsphereDataModelManager.getAssetsIncludingUserDecision(asset.name, allowToChooseNone = True)
            if assetInMindSphere:
                    assetInMindSphere = assetInMindSphere[0] #since the function 'getAssetsIncludingUserDecision' always returns a list ...

            asset.alreadyExistingInMindSphere = True if assetInMindSphere else False #Flag if this asset is already existing in MindSphere
            
            # In case there is no asset in MindSphere and also no parent has been defined in the import-definitions:
            if not asset.alreadyExistingInMindSphere and asset.parentAssetNameOrId in (None,""): #nothing has been specified in the input file
                asset.parentId = defaultParentObject.assetId
                parentInMindSphere = defaultParentObject
            
            # In case there is an asset in MindSphere and no parent Asset name was given, just take over the parent-Information of the existing asset.
            elif asset.alreadyExistingInMindSphere and asset.parentAssetNameOrId in (None,""):
                asset.parentId = assetInMindSphere.parentId  #There is no need to get the default values from config. Instead the parent will be derived from the existing asset
                parentInMindSphere = mindsphereDataModelManager.getAsset(assetId = asset.parentId)

            #The remaining case iis, that a parentInformation has been given in the import-source: 
            #This is why the importer will now try to get a parent from MindSphere based on this information
            else:
                parentInMindSphere = mindsphereDataModelManager.getAssetsIncludingUserDecision(asset.parentAssetNameOrId)
                if parentInMindSphere:
                    parentInMindSphere = parentInMindSphere[0] #since the function 'getAssetsIncludingUserDecision' always returns a list ...

            
            # Check input data for consistency
            if asset.alreadyExistingInMindSphere and parentInMindSphere:
                if assetInMindSphere.parentId != parentInMindSphere.assetId: 
                    print("Asset parentId: ",assetInMindSphere.parentId)
                    print("Parent assetId:",parentInMindSphere.assetId)
                    asset.error.addHardError(f"The asset to be imported ('{asset.name}') already exists, but the given parent id '{assetInMindSphere.parentId}' in the import-source definition does not fit the actually existing asset's parent '{parentInMindSphere.assetId}'")
                    raise continueOuterLoop  
           
            if not parentInMindSphere:
                # Check if parent is part of the input itself 
                parentObjectPartOfInputList = next((x for x in finalSortedAssetList if x.name == asset.parentAssetNameOrId),None )
                if not parentObjectPartOfInputList:
                    asset.error.addHardError("ParentName '{}' does not exist in MindSphere and is not part of input list".format(asset.parentAssetNameOrId))
                    raise continueOuterLoop
            else:
                asset.parentId = parentInMindSphere.assetId

            ## ASSET TYPE RELATED
            if not asset.alreadyExistingInMindSphere:
   
                if asset.typeId == None: #If no assetType definition is available at this point of the process, take over default assetType from config
                    asset.typeId = config.defaultAssetType

                if not "." in asset.typeId:
                    asset.typeId = helpers.deriveIdFromNameOrId(asset.typeId) 

                if asset.description == None: # ...same for Asset Description
                    asset.description = config.defaultAssetDescription

                if asset.assetTypeDescription == None: # ...same for Asset Type Description
                    asset.assetTypeDescription = config.defaultAssetTypeDescription
 
            #Check if AssetType of input data fits the one of the asset in MindSphere.
            #Therefore get assetType from Mindspheres assetTypeList first
            
            assetTypeInMindSphere = mindsphereDataModelManager.getAssetType(asset.typeId)
            if assetTypeInMindSphere: #Defined AssetType is existing in MindSphere
        
                #In case it is a standard asset type (identified by prefix "core."" in its name)
                if asset.typeId.startswith("core."):
                    asset.suitableAssetTypeExistingInMindSphere = True
                    continue

                aspectsInMindSphere = assetTypeInMindSphere.getAspects() 

                for localAspectName in asset.aspectDicts:

                    internalAspectName = asset.aspectDicts[localAspectName]["InternalAspectName"]
                    aspectInMindSphere = mindsphereDataModelManager.getAspect(internalAspectName)
                    if not aspectInMindSphere: #The Aspect is not definied within MindSphere, though the assetType is existing - that is actually quite bad - dont import this sh**:
                        asset.error.addHardError("Aspect's name '{}' does not fit the already existing definition within MindSphere's Aspects '{}'".format(internalAspectName,str([x["name"] for x in aspectsInMindSphere])))
                        raise continueOuterLoop

                    #Aspect is existing: Now compare parameters of aspect:
                    variableDefinitionsMindSphere = aspectInMindSphere.getVariables()
                    variableDefinitionsLocal = asset.aspectDicts[localAspectName]
                    if not validateAspectIntegrity(asset,internalAspectName, variableDefinitionsLocal,variableDefinitionsMindSphere):
                        #Something went wrong while validation Aspects for this AssetType, error has been  marked within asset object itself
                        #This is an inconstitent situation and cannot be fixed at this pont. 
                        # Therefore: Skip this asset and proceed with next one
                        raise continueOuterLoop

                asset.suitableAssetTypeExistingInMindSphere = True

            else: #The Asset Type does not exist yet
                asset.suitableAssetTypeExistingInMindSphere = False

                #Now define the AssetType with all its Aspects (independent if those aspects need to be imported or not):

                if asset.ancestorOfTypeId not in (None,""):
                    ancestorOfTypeId = asset.ancestorOfTypeId
                else: 
                    ancestorOfTypeId = config.defaultParentAssetTypeId #Todo: this could be renamed in the config to fit the new wroding: ancestorOfTypeId

                newAssetType = AssetType(asset.typeId, helpers.deriveNameFromNameOrId(asset.typeId), asset.assetTypeDescription, ancestorOfTypeId)
                #Now assign the relevant aspect information
                if logging in ("VERBOSE"):
                    print("*"*40)
                    
                for currentAspectFromInputDefinition in asset.aspectDicts:
                    currentAspect = asset.aspectDicts[currentAspectFromInputDefinition]
                    internalAspectName = currentAspect["InternalAspectName"]
                    currentAspectDescription = currentAspect["Aspect Description"]
                    variableDefinitions = currentAspect["VariableDefinition"]
                    if currentAspectDescription in (None,''):
                         currentAspectDescription = config.defaultAspectDescription
                        
                    newAspect = Aspect(internalAspectName,helpers.deriveIdFromNameOrId(internalAspectName), currentAspectDescription, aspectNameWithinAssetTypeContext = currentAspectFromInputDefinition )
                    for currentVariableName in variableDefinitions:
                        datatype = variableDefinitions[currentVariableName]["Datatype"]
                        unit = variableDefinitions[currentVariableName]["Unit"]
                        if datatype == "BOOLEAN":
                            flowLikelihood = variableDefinitions[currentVariableName].get("SimulatedFlowLikelihood")
                            newAspect.addVariable(currentVariableName, datatype, unit,flowLikelihood = flowLikelihood)

                        elif datatype in ("INT","DOUBLE"):
                            flowMin =variableDefinitions[currentVariableName].get("SimulatedFlowMin")
                            flowMax = variableDefinitions[currentVariableName].get("SimulatedFlowMax")
                            newAspect.addVariable(currentVariableName, datatype, unit,flowMin = flowMin, flowMax= flowMax)
                        else:
                            newAspect.addVariable(currentVariableName, datatype, unit)

                    aspectInMindSphere = mindsphereDataModelManager.getAspect(internalAspectName)

                    if not aspectInMindSphere: 
                        newAssetType.addAspectThatNeedsImporting(newAspect)

                        
                    else:
                        #Aspect is already existing in MindSphere: Now compare parameters of aspect:
                        variableDefinitionsMindSphere = aspectInMindSphere.getVariables()
                        variableDefinitionsLocal = asset.aspectDicts[currentAspectFromInputDefinition]
                        if not validateAspectIntegrity(asset,internalAspectName, variableDefinitionsLocal,variableDefinitionsMindSphere):
                            #Something went wrong while validation Aspects for this AssetType, error has been  marked within asset object itself
                            #This is an inconstitent situation and cannot be fixed at this pont. 
                            # Therefore: Skip this asset and proceed with next one
                            print(f"Validation of existing aspect in MindSphere {newAspect.name} failed for asset {asset.name}")
                            raise continueOuterLoop
                        # Either way (aspect existing or not) the aspect has to be added to the assetType's import definition
                    
                    if logging in ( "VERBOSE"):
                        print(f"adding Aspect '{newAspect.name}' with name '{newAspect.aspectNameWithinAssetTypeContext}' to '{newAssetType.name}' for asset '{asset.name}'")
                    newAssetType.addAspect(newAspect)

                if logging in ( "VERBOSE"):
                    print(f"Setting AssetType '{newAssetType.name}' for asset '{asset.name}'")


                asset.assetTypeToBeImported = newAssetType #This class should now contain the whole structure of assetType and Aspects
                

        except ContinueHelper:
            continue       
        except Exception as e: 
            print(e)
            traceback.print_exc()
            errorCounter += 1

    print("="*80)

    ##############
    # 5. Print current state, tell user, what will be imported and what won't (because of errors) and ask user if proceeding is required
    #Beim Abgleich mit den  MindSphere-Daten jeweils am Asset hinterlegen, welche Imports durchzuführen sind, um das Asset vollständig anzulegen
    #Nach dem Abgleich dem User anzeigen, wie viele Datensätze neu angelegt werden würden und auf Bestätigung warten
    #Weiter anzeigen, wie viele Errors/Diskrepanzen es gibt
    print("="*80)
    showInformationAndAskUserBeforeImporting(finalSortedAssetList, errorCounter)
    print("="*80)

    ##############
    # 6. Iterate through all Elements from the list and import dependencies. 
    # After creating an assettype or an Aspect a list will be populated. List name "I failed" and "I succeeded List", respectively including unique ID in success case (one list for assettypes, one list for assets) füllen und dann vor jedem neuen import checken, ob diese Listen bereits inhalt haben
    # Before importing anything else, those lists are alway checked before to avoid api calls
    # If the item has already been imported (success case) the ID can directly taken over from that list and another import will be skipped.
    #In case something went wrong, set asset import state to failure
    print("="*80)
    print("Starting import now...")
    # Dont process Error-Assets for now
    importAssetStructureToMindSphere(finalSortedAssetList)
    print("="*80)
    # 7. Rewalk the list and import timeseries data for all successfull assets 
    # 

    ##############
    # 8. Show Status after Importing
    evaluateStatusAfterImporting(finalSortedAssetList)

    # 9. Create VFC Simulation flows:
    createVFCflows(finalSortedAssetList)


   
