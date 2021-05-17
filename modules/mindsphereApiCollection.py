import json
import time
from pprint import pprint
import traceback
import sys
import modules.readConfig as config
import os
import pdb
#Custom Modules
from modules.apiHandler import wrapApiCall
from modules.helpers import SimpleError
from os.path import join


#######################################
########### MODULE CONFIG #############
#######################################
# The following block loads parameters from the config an provides them in an easy to use way in this modul:
# Instead of config.<parametername> you can just use <parametername> afterwards
thisModule = sys.modules[__name__]
requiredParamters= "useLocalFilesToPreserveMindSphereData, tenantname, logging"
config.setSimpleConfigParametersToModule(thisModule, requiredParamters)


fetchFromFile = useLocalFilesToPreserveMindSphereData
overwriteResultOfAspectImportWithAFakedSuccess = False #THIS SHOULD NOT BE SET TO TRUE!!!


# File paths 
assetsFilePath =        os.path.join('temp',tenantname + '_assets.txt')
assetTypesFilePath =    os.path.join('temp',tenantname + '_assetTypes.txt')
aspectsFilePath =       os.path.join('temp',tenantname + '_aspects.txt')
agentsFilePath =        os.path.join('temp',tenantname + '_agents.txt')

genericMindSphereObjectMapper = {
"assets" :      {"url":"/api/assetmanagement/v3/assets?size=500",       "displayName":"Asset",      "filepath":assetsFilePath},
"assetTypes" :  {"url":"/api/assetmanagement/v3/assettypes?size=500",   "displayName":"Asset-Type", "filepath":assetTypesFilePath},
"aspectTypes" : {"url":"/api/assetmanagement/v3/aspecttypes?size=500",  "displayName":"Aspect",     "filepath":aspectsFilePath}

}


############## GET ####################

#Device related stuff
def getDataModelObjectsFromMindSphere(mindSphereObjectType):
    currentObject = genericMindSphereObjectMapper[mindSphereObjectType]
    currentFilePath = currentObject["filepath"]
    currentApiPath = currentObject["url"]
    currentDisplayName = currentObject["displayName"]

    if fetchFromFile:
        try:
            with open(currentFilePath) as json_file:  
                objectsFromMindSphere = json.load(json_file)
                print(f"... loading {currentDisplayName}-Data from local file now")
            return objectsFromMindSphere

        except FileNotFoundError:
            print(f"... loading {currentDisplayName}-Data from MindSphere Cloud now")
        except:
            traceback.print_exc()

    objectsFromMindSphere = []
    totalElements = 0
    if logging in ("INFO"):
        print("Getting page 1 for {}s now".format(currentDisplayName))

    response = wrapApiCall(currentApiPath,"GET","{}")

    if int(response["responseStatusCode"])>300:
        print("Something went wrong with getting {}s from Mindsphere. Exiting now...".format(currentDisplayName))
        print(response["responseText"])
        exit(-1) #Todo: Error Handling einbauen?!

    result = response["result"]

    if result["_embedded"][mindSphereObjectType]:
        objectsFromMindSphere.extend(result["_embedded"][mindSphereObjectType])

    if result["page"]["totalPages"]: 
        numberOfReturnPages =  int(result["page"]["totalPages"])
    if result["page"]["totalElements"]: 
        totalElements = int(result["page"]["totalElements"])

    if numberOfReturnPages > 1:
        nextResult = result
        for x in range(2, numberOfReturnPages + 1):
            if logging in ("INFO", "VERBOSE"):
                print("Getting page {} for {}s now".format(x,currentDisplayName))
            if nextResult["_links"]["next"]["href"]:
                nextLink = nextResult["_links"]["next"]["href"]
                nextResult = wrapApiCall(nextLink,"GET","{}")["result"]
                objectsFromMindSphere.extend(nextResult["_embedded"][mindSphereObjectType])
                if nextResult["page"]["totalElements"]: 
                    totalElements = int(nextResult["page"]["totalElements"])

    if len(objectsFromMindSphere) !=  totalElements:
        print("Fetched elements ({}) differ from number of elements according to APIs response PAGE-TOTALELEMENTS ({})".format(len(objectsFromMindSphere), totalElements))
        print("This bodes ill and therefore ...exiting now")
        exit(-1)
       
    if fetchFromFile:
        with open(currentFilePath, 'w') as outfile:  
            json.dump(objectsFromMindSphere, outfile)

    return objectsFromMindSphere

def getAssetsFromMindSphere():
    simpleError = SimpleError()
    assetsFromMindSphere = getDataModelObjectsFromMindSphere('assets')
    return (simpleError, assetsFromMindSphere)

def getAssetTypesFromMindSphere():
    simpleError = SimpleError()
    assetTypesFromMindSphere = getDataModelObjectsFromMindSphere('assetTypes')
    return (simpleError, assetTypesFromMindSphere)

def getAspectsFromMindSphere():
    simpleError = SimpleError()
    aspectsFromMindSphere = getDataModelObjectsFromMindSphere('aspectTypes')
    return (simpleError, aspectsFromMindSphere)


#Agent related stuff
def getAgentsFromMindSphere():
    # Attention: The Agent Management API is not in scope of assetmanager-Application -> Update 2020 -> this seems to be not true anymore
    # This can be reworked with trying to identify agents via the asset's assetType (if the type is core.mcXXX: it is an agent)
    simpleError = SimpleError()

    if fetchFromFile:
        try:
            with open(agentsFilePath) as json_file: 
                print(f"... loading Agent-Data from local file now") 
                agentsFromMindSphere = json.load(json_file)

            return (simpleError,agentsFromMindSphere)

        except FileNotFoundError:
            print(f"... loading Agent-Data from MindSphere Cloud now")
            pass
        except:
            traceback.print_exc()
            
    
    agentsFromMindSphere = []
    totalAgents = 0
    currentPageNumber = 0
    result = wrapApiCall("/api/agentmanagement/v3/agents?page="+ str(currentPageNumber) + "&size=100&sort=name,asc","GET","{}")["result"]
    
    if logging in ("INFO"):
        print("Getting page {} for Agents now".format(currentPageNumber +1))
    if result and "content" in result:
        if result["content"]:
            agentsFromMindSphere.extend(result["content"])

        if result["totalPages"]: 
            numberOfReturnPages =  int(result["totalPages"])
        if result["totalElements"]: 
            totalAgents = int(result["totalElements"])

        if numberOfReturnPages > 1:
            nextResult = result
            for x in range(2, numberOfReturnPages + 1):
                currentPageNumber += 1
                if logging in ("INFO", "VERBOSE"):
                    print("Getting page {} for Agents now".format(x))
                nextLink = "/api/agentmanagement/v3/agents?page="+ str(currentPageNumber) + "&size=100&sort=name,asc"
                nextResult = wrapApiCall(nextLink,"GET","{}")["result"]
                agentsFromMindSphere.extend(nextResult["content"])
                if nextResult["totalElements"]: 
                    totalAgents = int(nextResult["totalElements"])

        if len(agentsFromMindSphere) !=  totalAgents:
            simpleError.addError("Fetched elements ({}) differ from number of elements according to APIs response PAGE-TOTALELEMENTS ({})".format(len(agentsFromMindSphere), totalAgents))

        if fetchFromFile:
            with open(agentsFilePath, 'w') as outfile:  
                json.dump(agentsFromMindSphere, outfile)

    return (simpleError,agentsFromMindSphere)

def getDatasourceConfigForAgentAsset(agentAsset):
    simpleError = SimpleError()

    datasourceConfiguration = None
    if agentAsset.typeId != "core.mclib":
        # wenn es kein Lib Agent ist, schaut das Abrufen der vollen Datasource Konfig so aus
        returnValue = wrapApiCall("/api/mindconnectdevicemanagement/v3/devices/"+ agentAsset.assetId + "/dataConfig","GET",assetTypeToDeriveApplicationScope = agentAsset.typeId)
        # Der mindconnctdevicemanagement Endpunkt liefert bei mclib Agenten keine Konfig zurück: "Device configuration does not exist for given assetId xy".
        # Für die MindConnect Lib-Elemente muss man den Agentmanagement-Endpunkt verwenden. Dieser ginge zwar auch für mcnano und mciot2040 aber man erhält dort nicht die volle Info.
        # Deshalb nimmt man für NANO und IoT2040 den mindconnectdevicemanagement-Endpunkt
    else:
        returnValue = wrapApiCall("/api/agentmanagement/v3/agents/"+ agentAsset.assetId + "/dataSourceConfiguration","GET",assetTypeToDeriveApplicationScope = agentAsset.typeId)
    if not returnValue:
        print("Something went wrong with getting datasource Config. Result of API call was empty...")
        exit(-1)
    
    datasourceConfiguration = returnValue["result"]

    return (simpleError,datasourceConfiguration )


def getAllDatapointMappingsForAgentAsset(agentAsset):
    simpleError = SimpleError()
    #TODO: IMPORTANT: This will currently return maximum 500 (or less, no idea how much are allowed) mapping entries, due to the laziness of the coder. 
    #In case you have more mappings, this needs some work
    datapointMappingConfigurations = None
    result = wrapApiCall('/api/mindconnect/v3/dataPointMappings?filter={"agentId":"'+ agentAsset.assetId + '"}&page=0&size=500',"GET",assetTypeToDeriveApplicationScope = agentAsset.typeId)["result"]
    if not result:
        print("Something went wrong with getting datasource mapping config. Result of API call was empty...maybe this is even okay, if no mappings exist")
        exit(-1)
    datapointMappingConfigurations = result["content"]

    return (simpleError,datapointMappingConfigurations )

def getDeviceConfigurationForAgentAsset(agentAsset):
    # This queries something like /api/mindconnectdevicemanagement/v3/devices/<agentAssetID> 
    # If Agent is Nano or Iot2040 it will return information about Boarding-Status, IP-Configuration, DHCP, Proxy, SerialNumber:
    # For MC Lib Elements there won't be a Device-Config 
    # If you ask for it in case of a MC Lib Asset, you will get a reply "Insufficient scope for this resource" when using session cookies
    # When using app credential token for it, you will receive this 'Device configuration does not exist for given assetId'
    # Über /api/mindconnectdevicemanagement/v3/devices/<agentAssetID>/firmware/info könnte man auch noch die Firmware abrufen
    simpleError = SimpleError()
    deviceInformation = None

    if agentAsset.typeId != "core.mclib":
        result = wrapApiCall("/api/mindconnectdevicemanagement/v3/devices/"+ agentAsset.assetId,"GET",assetTypeToDeriveApplicationScope = agentAsset.typeId)["result"]

        if not result:
            print("Something went wrong with getting datasource Config. Result of API call was empty...")
            exit(-1)

        deviceInformation = result

    return (simpleError,deviceInformation)


############# CREATE ##################

def createNewAssetInMindSphere(asset):
    relatedFileName = assetsFilePath
    print(" °°°° Importing Asset '{}' now ... ".format(asset.name))
    bodyAsJson = {}
    tenantname = config.tenantname

    name = asset.name
    if asset.typeId not in (None,""):
        assetType = asset.typeId
    else:
        assetType = config.defaultAssetType  #If nothing has been provided: Get Default (shouldnt happen at that point)

    if not assetType.startswith("core.") and not assetType.startswith(tenantname + "."):
        assetTypeWithPrefix = tenantname + "." + assetType #Add TenantPrefix to AssetType
    
    else: #prefix is already existing
        assetTypeWithPrefix = assetType 

    if asset.assetDescription not in (None,""):
        assetDescription = asset.assetDescription
    else:
        assetDescription = config.defaultAssetDescription  #If nothing has been provided: Get Default

    if asset.parentId not in (None,""):
        parentId = asset.parentId
    else:
        parentId = config.defaultParentId #If nothing has been provided: Get Default

    
    bodyAsJson["name"] = name    
    bodyAsJson["typeId"] = assetTypeWithPrefix    
    bodyAsJson["description"] = assetDescription    
    bodyAsJson["parentId"] = parentId
    if logging == "VERBOSE":
        print("Trying to import Asset with following body")
        print(bodyAsJson)
    
    returnValue = wrapApiCall("/api/assetmanagement/v3/assets", "POST", bodyAsJson)
    result = returnValue["result"]
    statusCode = returnValue["responseStatusCode"]
    responseText = returnValue["responseText"]
    
    if statusCode >=200 and statusCode <300:
        if fetchFromFile:

            responseAsDict = json.loads(responseText)

            asset.assetId = responseAsDict["assetId"] #TODO -> Auch bei anderen Create Prozessen noch die ID und weitere Parameter an die Klasse hängen

            try:
                with open(relatedFileName) as json_file:  
                    currentFileContent = json.load(json_file)

                currentFileContent.append(responseAsDict)

                with open(relatedFileName, 'w') as outfile:  
                    json.dump(currentFileContent, outfile)  
            
            except Exception:
                traceback.print_exc()


    returnDict = dict()
    returnDict["response"] = result
    returnDict["statusCode"] =statusCode
    returnDict["responseText"] = responseText
    
    return returnDict

def createNewAssetTypeInMindSphere(assetType):

    relatedFileName = assetTypesFilePath

    print(" °°°° Importing AssetType '{}' now ... ".format(assetType.name))
    bodyAsJson = {}
    
    #handling of core asset-types should not be needed, since it should never happen, that the importer tries to import a core.assetType ...should... ;)
    if assetType.id.startswith("core"):
        print("It should never happen, that the importer tries to import a core.asset ...something is wrong here in the program's logic, please do complain somewhere...exiting now...")
        exit(-1) 
    
    if assetType.description in (None,""):
        assetType.description = config.defaultAssetDescription  #If nothing has been provided: Get Default

    aspectsForImport = []

    for aspect in assetType.getAspects():

        aspectDict = {"name": aspect.aspectNameWithinAssetTypeContext,"aspectTypeId":aspect.id}
        aspectsForImport.append(aspectDict)

    if assetType.ancestorOfTypeId in (None,""):
        assetType.ancestorOfTypeId = config.defaultParentAssetTypeId

    bodyAsJson["name"] = assetType.name
    bodyAsJson["id"] = assetType.id       
    bodyAsJson["parentTypeId"] = assetType.ancestorOfTypeId
    bodyAsJson["description"] = assetType.description  
    bodyAsJson["aspects"] = aspectsForImport
    bodyAsJson["instantiable"] = "true"
    bodyAsJson["scope"] = "private"


    if logging == "VERBOSE":
        print("Trying to import AssetType with following body")


    returnValue = wrapApiCall("/api/assetmanagement/v3/assettypes/" + assetType.id, "PUT", bodyAsJson)

    result = returnValue["result"]
    statusCode = returnValue["responseStatusCode"]
    responseText = returnValue["responseText"]

    if statusCode >=200 and statusCode <300:
        if fetchFromFile:
            
            responseAsDict = json.loads(responseText)

            try:
                with open(relatedFileName) as json_file:  
                    currentFileContent = json.load(json_file)

                currentFileContent.append(responseAsDict)

                with open(relatedFileName, 'w') as outfile:  
                    json.dump(currentFileContent, outfile)  
            
            except Exception:
                traceback.print_exc()

    returnDict = dict()
    returnDict["response"] = result
    returnDict["statusCode"] = statusCode
    returnDict["responseText"] = responseText
    
    return returnDict

def createNewAspectInMindSphere(aspect):
    relatedFileName = aspectsFilePath
    print(" °°°° Importing Aspect '{}' now ... ".format(aspect.name))
    bodyAsJson = {}

    if aspect.description not in (None,""):
        aspectDescription = aspect.description
    else:
        aspectDescription = config.defaultAspectDescription  #If nothing has been provided: Get Default

    variablesForImport = []
    for variable in aspect.getVariables():
        variableDict = {"name": variable.name,"dataType": variable.dataType,"unit": variable.unit}
        
        if variable.dataType.lower() == "string":
            variableDict["length"] = config.maxLengthForStringVariableCreation

        variablesForImport.append(variableDict)
        
    bodyAsJson["name"] = aspect.name     
    bodyAsJson["description"] = aspectDescription  
    bodyAsJson["variables"] = variablesForImport
    bodyAsJson["scope"] = aspect.scope
    bodyAsJson["category"] = aspect.category

    if logging == "VERBOSE":
        print("Trying to import aspect with following body")
        print(bodyAsJson)
    
    

    returnValue = wrapApiCall("/api/assetmanagement/v3/aspecttypes/" + aspect.id, "PUT", bodyAsJson)

    result = returnValue["result"]
    statusCode = returnValue["responseStatusCode"]
    responseText = returnValue["responseText"]

    if statusCode >=200 and statusCode <300:
        if fetchFromFile:
            
            responseAsDict = json.loads(responseText)

            try:
                with open(relatedFileName) as json_file:  
                    currentFileContent = json.load(json_file)

                currentFileContent.append(responseAsDict)

                with open(relatedFileName, 'w') as outfile:  
                    json.dump(currentFileContent, outfile)  
            
            except Exception:
                traceback.print_exc()

    returnDict = dict()
    returnDict["response"] = result
    returnDict["statusCode"] = statusCode
    returnDict["responseText"] = responseText
    
    return returnDict

def createDataSourceDict(dataSource, typeId):
    dataSourceDict = {}
    dataPointList = []

    dataSourceDict["name"] = dataSource.name
    dataSourceDict["description"] = dataSource.description

    if typeId == "core.mclib":

        """Style of JSON:
        {"name":"asdf","description":"bsdf",
        "dataPoints":
            [
                {"id":"1606389468639",
                "name":"goebel",
                "description":"boebl",
                "type":"DOUBLE",
                "unit":"rudi",
                "customData":null}
            ]
            ,
        "customData":null}
        """


        dataSourceDict["customData"] = dataSource.customData

        for dataPoint in dataSource.dataPoints:
            dataPointDict = {}
            dataPointDict["customData"] = dataPoint.customData
            dataPointDict["name"] = dataPoint.name
            dataPointDict["unit"] = dataPoint.unit
            dataPointDict["type"] = dataPoint.dataType
            dataPointDict["description"] = dataPoint.description
            dataPointDict["id"] = dataPoint.dataPointId #This is an arbitrary identifier that should be unique and is used for datapoint mappings later.

            dataPointList.append(dataPointDict)

    else: # Mind Connect Nano or Iot204
        """Style of JSON:
        {
        "id":"8933556c-0e06-4995-a1e7-0104933c4821",
        "name":"asdf",
        "description":"",
        "protocol":"S7",
        "readCycleInSeconds":"60",
        "protocolData":{"ipAddress":"127.0.0.4"},
        "dataPoints":
            [
                    {
                    "id": "984866c4-d2c6-4bcf-83e4-3f4a0b9bd829",
                    "dataPointId": "9dc409f108d24",
                    "name": "Ready",
                    "description": "",
                    "unit": "-",
                    "dataType": "BOOLEAN",
                    "dataPointData": {
                        "address": "DB435.DBX396.1",
                        "hysteresis": null,
                        "onDataChanged": false
                    }
                    
            ]
        }
        """
        #dataSourceDict["id"] = "" #The id will be left out from the datapoint-dictionay and it wil be auto generated
        protocol = dataSource.protocol
        dataSourceDict["protocolData"] = {}
        dataSourceDict["protocol"] = protocol

        dataSourceDict["readCycleInSeconds"] = dataSource.readCycleInSeconds
        
        if protocol == "OPCUA":

            dataSourceDict["protocolData"]["opcUaServerName"] =dataSource.opcUaServerName
            dataSourceDict["protocolData"]["opcUaServerAddress"]= dataSource.opcUaServerAddress
            dataSourceDict["protocolData"]["opcUaServerIPAddress"]= dataSource.opcUaServerIPAddress
            dataSourceDict["protocolData"]["opcUaCertificateMetadata"]= dataSource.opcUaCertificateMetadata
            dataSourceDict["protocolData"]["opcUaCertificate"]= {}
            dataSourceDict["protocolData"]["opcUaAuthenticationType"] = None
            dataSourceDict["protocolData"]["opcUaSecurityMode"] = dataSource.opcUaSecurityMode
            dataSourceDict["protocolData"]["opcUaUsername"] = dataSource.opcUaUsername
            dataSourceDict["protocolData"]["opcUaPassword"] = dataSource.opcUaPassword
            dataSourceDict["protocolData"]["enableEvents"] = False

        if protocol == "S7":
            dataSourceDict["protocolData"]["ipAddress"] = dataSource.ipAddress
            dataSourceDict["protocolData"]["manualRackAndSlot"] = dataSource.manualRackAndSlot
            dataSourceDict["protocolData"]["rackNumber"] = dataSource.rackNumber
            dataSourceDict["protocolData"]["slotNumber"] = dataSource.slotNumber
  
        
    
        for dataPoint in dataSource.dataPoints:
            dataPointDict = {}
            dataPointDict["dataPointId"] = dataPoint.dataPointId # This is an arbitrary identifier that should be unique and is used for datapoint mappings later.
            dataPointDict["name"] = dataPoint.name
            dataPointDict["unit"] = dataPoint.unit
            dataPointDict["dataType"] = dataPoint.dataType
            dataPointDict["description"] = dataPoint.description
            dataPointDict["dataPointData"] = {
                    "address": dataPoint.address,
                    "hysteresis": dataPoint.hysteresis,
                    "onDataChanged": dataPoint.onDataChanged
                     }

            if protocol == "S7" and dataPoint.acquisitionType:
                dataPointDict["dataPointData"]["acquisitionType"] = dataPoint.acquisitionType 
            
            dataPointList.append(dataPointDict)

    dataSourceDict["dataPoints"] = dataPointList

    return dataSourceDict

#######################################

def assignReceivedDatapointIds(agentAsset, responseAsDict):
    # Probably this is not needed at all, since mappings are only relating to the dataPointIds which are given through the user and which are therefore already known to the python-datamodel
    pass

#######################################



#######################################

def createCompletelyNewDataSourcesAndDatapointDefinition(agentAsset):
    latestETag = None
    simpleError,dataSourceConfiguration = getDatasourceConfigForAgentAsset(agentAsset)
    listWithAllDataSources = [] 
    

    if agentAsset.typeId != "core.mclib":
        if dataSourceConfiguration["uploadCycle"] in ("",None):
            dataSourceConfiguration["uploadCycle"] = agentAsset.agentData.uploadCycle 

    else:
        if dataSourceConfiguration["configurationId"] in ("","null",None):
            dataSourceConfiguration["configurationId"]  =  str(int(time.time())) # aktuelle epoch zeit als zufällige id hinerlegen
        latestETag = dataSourceConfiguration["eTag"]
    


    for dataSource in agentAsset.agentData.dataSources:
        listWithAllDataSources.append(createDataSourceDict(dataSource,agentAsset.typeId))
    
    dataSourceConfiguration["dataSources"] = listWithAllDataSources

    return _putDatasourceAndDatapointDefinition(agentAsset, dataSourceConfiguration, latestETag)

#######################################

def addDataSourcesAndDatapointDefinition(agentAsset, newDataSources):
    latestETag = None
    simpleError,dataSourceConfiguration = getDatasourceConfigForAgentAsset(agentAsset)


    if agentAsset.typeId == "core.mclib":
        latestETag = dataSourceConfiguration["eTag"]

    for dataSource in newDataSources:
        dataSourceDictonary = createDataSourceDict(dataSource,agentAsset.typeId)
        if any(dataSourceDictonary["name"] in dataSource for dataSource in dataSourceConfiguration):
            print(f"ATTENTION: Datasource with name '{dataSourceDictonary['name']}' is already existing for the agent {agentAsset.name}. It will not be added.")
            continue
        dataSourceConfiguration["dataSources"].append(dataSourceDictonary)

    return _putDatasourceAndDatapointDefinition(agentAsset, dataSourceConfiguration, latestETag)

#######################################

def deleteDataSourceDefinitions(agentAsset, dataSourceNamesToBeDelete):
    latestETag = None
    simpleError, dataSourceConfiguration = getDatasourceConfigForAgentAsset(agentAsset)
    if agentAsset.typeId == "core.mclib":
        latestETag = dataSourceConfiguration["eTag"]

    dataSourceConfiguration.pop('id', None)
    dataSourceConfiguration.pop('eTag', None)

    dataSourceConfiguration["dataSources"] = [datasource for datasource in dataSourceConfiguration["dataSources"] if datasource["name"] not in dataSourceNamesToBeDelete]
    return _putDatasourceAndDatapointDefinition(agentAsset, dataSourceConfiguration, latestETag)

#######################################

def _putDatasourceAndDatapointDefinition(agentAsset, dataSourceDefinition, latestETag = None):
    ifMatchHeader = {}
    if latestETag:
        ifMatchHeader = {"if-match":str(latestETag)}

    additionalHeaders = ifMatchHeader

    dataSourceDefinition.pop('id', None)
    dataSourceDefinition.pop('eTag', None)
    bodyAsJson = dataSourceDefinition

    if logging == "VERBOSE":
        print("Trying to import DataSource and Datapoints with following body")
        print(bodyAsJson)

    # for mc lib elements and etag needs to be set using a PUT method 
    if agentAsset.typeId != "core.mclib":
        # if it is no mc lib agent, the full datasource config will be fetched like this:
        returnValue = wrapApiCall("/api/mindconnectdevicemanagement/v3/devices/"+ agentAsset.assetId + "/dataConfig","PUT",bodyAsJson,assetTypeToDeriveApplicationScope = agentAsset.typeId, additionalHeaders= additionalHeaders)
        # mindconnctdevicemanagement endpoint does not provide config for mclib agents: "Device configuration does not exist for given assetId xy".
        # Therefore MindConnect Lib-Elements require the Agentmanagement-endpoint. This would also work for mcnano and mciot204, but with less information.
        # As a consequence for NANO and IoT2040 mindconnectdevicemanagement-endpoint will be used
    else:
        returnValue = wrapApiCall("/api/agentmanagement/v3/agents/"+ agentAsset.assetId + "/dataSourceConfiguration","PUT",bodyAsJson,assetTypeToDeriveApplicationScope = agentAsset.typeId, additionalHeaders= additionalHeaders)
    #The received datapoint IDs need to be saved in the dataPointClass, so that the mappings can applied

    if not returnValue:
        print("Something went wrong with creating the datasource configuration. Result of API call was empty...")
        exit(-1)

   
    result = returnValue["result"]
    statusCode = returnValue["responseStatusCode"]
    responseText = returnValue["responseText"]
    
    if statusCode >=200 and statusCode <300:

        responseAsDict = json.loads(responseText)
        assignReceivedDatapointIds(agentAsset, responseAsDict)
  
    returnDict = dict()
    returnDict["response"] = result
    returnDict["statusCode"] =statusCode
    returnDict["responseText"] = responseText
    
    return returnDict

#######################################

def createDatapointMapping(dataPointMapping, agentAsset):
    print(f" °°°° Creating DatpointMapping for variable '{dataPointMapping.variableName}' between '{agentAsset.name}' and '{dataPointMapping.targetAsset.name}' now ... ")
    #The received datapoint mapping ID does probably not matter and does not be saved.
    if not dataPointMapping.agentId:
        dataPointMapping.agentId = dataPointMapping.agentAsset.assetId
    bodyAsJson = {
    "agentId": dataPointMapping.agentId,
    "dataPointId": dataPointMapping.dataPointId,
    "entityId": dataPointMapping.targetAsset.assetId,
    "propertySetName":dataPointMapping.aspectId,
    "propertyName": dataPointMapping.variableName
    }
    
    if logging == "VERBOSE":
        print("Trying to import Datapoint Mapping with following body")
        print(bodyAsJson)

    if agentAsset.typeId != "core.mclib":          
        returnValue = wrapApiCall("/api/mindconnect/v3/dataPointMappings","POST",bodyAsJson, assetTypeToDeriveApplicationScope = agentAsset.typeId)

    else:
        bodyAsJson["keepMapping"] = True
        returnValue = wrapApiCall("/api/mindconnect/v3/dataPointMappings","POST",bodyAsJson, assetTypeToDeriveApplicationScope = agentAsset.typeId)


    
    if not returnValue:
        print("Something went wrong with creating a datapoint mapping. Result of API call was empty...")
        exit(-1)

   
    result = returnValue["result"]
    statusCode = returnValue["responseStatusCode"]
    responseText = returnValue["responseText"]
    
    if statusCode >=200 and statusCode <300:

        responseAsDict = json.loads(responseText)
        assignReceivedDatapointIds(agentAsset, responseAsDict)
  
    returnDict = dict()
    returnDict["response"] = result
    returnDict["statusCode"] =statusCode
    returnDict["responseText"] = responseText
    
    return returnDict

#######################################

def applyMappingConfigurationToDevice(agentAsset):
    returnDict = dict()
    bodyAsJson = {}
    print(f" °°°° Applying changes to hardware device '{agentAsset.name}' now ...")
    if agentAsset.typeId != "core.mclib": #apply changes is only available for hardware devices
        returnValue = wrapApiCall(f"/api/mindconnectdevicemanagement/v3/devices/{agentAsset.assetId}/applyChanges","POST",bodyAsJson, assetTypeToDeriveApplicationScope = agentAsset.typeId)


        if not returnValue:        
            print("Something went wrong with applying mapping Configuration. Result of API call was empty...")
        exit(-1)

   
        result = returnValue["result"]
        statusCode = returnValue["responseStatusCode"]
        responseText = returnValue["responseText"]
        
        if statusCode >=200 and statusCode <300:

            responseAsDict = json.loads(responseText)
            assignReceivedDatapointIds(agentAsset, responseAsDict)
    
        
        returnDict["response"] = result
        returnDict["statusCode"] =statusCode
        returnDict["responseText"] = responseText
        
        return returnDict
    else:
        returnDict["response"] = "There is no option to apply config for mc.lib elements"
        returnDict["statusCode"] =  123
        returnDict["responseText"] = "There is no option to apply config for mc.lib elements"


#######################################

def initializeAgentInMindSphere(agentAsset):
    if not agentAsset.alreadyExistingInMindSphere:
        createNewAgent(agentAsset)

    createCompletelyNewDataSourcesAndDatapointDefinition(agentAsset)

#######################################

#######################################


def createNewAgent(asset):

    if asset.typeId == "core.mclib":
        bodyAsJson = {
            "name":asset.assetId,
            "securityProfile":asset.agentData.securityProfile,
            "entityId":asset.assetId
        }

        if logging in ("VERBOSE"):
            print("Trying to initialize DeviceConfig with following body")
            print(bodyAsJson)

        returnValue = wrapApiCall("/api/agentmanagement/v3/agents", "POST", bodyAsJson, assetTypeToDeriveApplicationScope=asset.typeId)

    else:
        networkList = []
        for network in asset.agentData.deviceConfiguration.networkInterfaces:
            currentNetworkDict = {}
            currentNetworkDict["name"] = network.name
            
            if network.DHCP:
                currentNetworkDict["DHCP"] = {"enabled":True}
                currentNetworkDict["static"] ={}
            else:
                currentNetworkDict["DHCP"] = {"enabled":False}
                currentNetworkDict["IPv4"] =  network.IPv4
                currentNetworkDict["IPv6"] =  network.IPv6
                currentNetworkDict["DNS"] = network.DNS
                currentNetworkDict["SubnetMask"] =  network.subnetMask
                currentNetworkDict["Gateway"] = network.gateway
            networkList.append(currentNetworkDict)

        deviceDict = {
        "serialNumber" : asset.agentData.deviceConfiguration.serialNumber,
        "deviceType":"NANO" if asset.typeId == "core.mcnano" else "IOT2040",
        "networkInterfaces" : networkList
        }

        bodyAsJson = {
            "assetId" : asset.assetId,
            "device" : deviceDict,
            "agent" : {"name":asset.assetId, "proxy" : {}}
        }

        if logging in ("VERBOSE"):
            print("Trying to initialize DeviceConfig with following body")
            print(bodyAsJson)
        returnValue = wrapApiCall("/api/mindconnectdevicemanagement/v3/devices" , "POST", bodyAsJson,assetTypeToDeriveApplicationScope=asset.typeId) #Todo: if this should be able to update existing agents device configs, this needs to be a put request

    
    result = returnValue["result"]
    statusCode = returnValue["responseStatusCode"]
    responseText = returnValue["responseText"]
    

    returnDict = dict()
    returnDict["response"] = result
    returnDict["statusCode"] =statusCode
    returnDict["responseText"] = responseText
    
    return returnDict



#######################################

def getOnboardingKey(agentAsset):

    bodyAsJson= {}

    if agentAsset.typeId == "core.mclib":
       
        returnValue = wrapApiCall("/api/agentmanagement/v3/agents/" + agentAsset.assetId + "/boarding/configuration", "GET", bodyAsJson,assetTypeToDeriveApplicationScope=agentAsset.typeId)

    else:
        returnValue = wrapApiCall("/api/mindconnectdevicemanagement/v3/devices/" + agentAsset.assetId + "/onboardingConfig?encrypted=false", "GET", bodyAsJson,assetTypeToDeriveApplicationScope=agentAsset.typeId)


    result = returnValue["result"]
    statusCode = returnValue["responseStatusCode"]
    responseText = returnValue["responseText"]

    returnDict = dict()
    returnDict["response"] = result
    returnDict["statusCode"] =statusCode
    returnDict["responseText"] = responseText
    
    return returnDict


#######################################


############# DELETE ##################

def deleteAsset(asset):
    
    relatedFileName = assetsFilePath
    print(" °°°° Deleting asset '{}' now ... ".format(asset.name))
    bodyAsJson = {}

    ifMatchHeader = {"if-match":str(asset.etag)}

    returnValue = wrapApiCall("/api/assetmanagement/v3/assets/" + asset.assetId, "DELETE", bodyAsJson, additionalHeaders=ifMatchHeader)

    result = returnValue["result"]
    statusCode = returnValue["responseStatusCode"]
    responseText = returnValue["responseText"]
    
    if statusCode >=200 and statusCode <300:

        if fetchFromFile:

            try:
                with open(relatedFileName) as json_file:  
                    oldFileContent = json.load(json_file)
                    
                currentFileContent = [d for d in oldFileContent if d.get('assetId') != asset.assetId]


                with open(relatedFileName, 'w') as outfile:  
                    json.dump(currentFileContent, outfile)  
            
            except Exception:
                traceback.print_exc()

    returnDict = dict()
    returnDict["response"] = result
    returnDict["statusCode"] =statusCode
    returnDict["responseText"] = responseText
    
    return returnDict

#######################################

def deleteAssetType(assetType):
    relatedFileName = assetTypesFilePath
    print(" °°°° Deleting assetType '{}' now ... ".format(assetType.name))
    bodyAsJson = {}

    ifMatchHeader = {"if-match":str(assetType.etag)}

    returnValue = wrapApiCall("/api/assetmanagement/v3/assettypes/" + assetType.id, "DELETE", bodyAsJson, additionalHeaders=ifMatchHeader)

    result = returnValue["result"]
    statusCode = returnValue["responseStatusCode"]
    responseText = returnValue["responseText"]

    if statusCode >=200 and statusCode <300:
    
        if fetchFromFile:

            try:
                with open(relatedFileName) as json_file:  
                    oldFileContent = json.load(json_file)
                    
                currentFileContent = [d for d in oldFileContent if d.get('id') != assetType.id]

                with open(relatedFileName, 'w') as outfile:  
                    json.dump(currentFileContent, outfile)  
            
            except Exception:
                traceback.print_exc()
    returnDict = dict()
    returnDict["response"] = result
    returnDict["statusCode"] =statusCode
    returnDict["responseText"] = responseText
    
    return returnDict

#######################################

def deleteAspect(aspect):
    relatedFileName = aspectsFilePath
    print(" °°°° Deleting aspect '{}' now ... ".format(aspect.name))
    bodyAsJson = {}

    ifMatchHeader = {"if-match":str(aspect.etag)}

    returnValue = wrapApiCall("/api/assetmanagement/v3/aspecttypes/" + aspect.id, "DELETE", bodyAsJson, additionalHeaders=ifMatchHeader)

    result = returnValue["result"]
    statusCode = returnValue["responseStatusCode"]
    responseText = returnValue["responseText"]

    if statusCode >=200 and statusCode <300:
    
        if fetchFromFile:

            try:
                with open(relatedFileName) as json_file:  
                    oldFileContent = json.load(json_file)
                    
                currentFileContent = [d for d in oldFileContent if d.get('id') != aspect.id]


                with open(relatedFileName, 'w') as outfile:  
                    json.dump(currentFileContent, outfile)  
            
            except Exception:
                traceback.print_exc()
    returnDict = dict()
    returnDict["response"] = result
    returnDict["statusCode"] =statusCode
    returnDict["responseText"] = responseText
    
    return returnDict    


############# Timeseries ##################

def writeTimeSeriesData(assetId,aspectName,dataList):
    """ URL = /timeseries/{entityId}/{propertySetName}
        Body needs to be a list of dictionaries:
        [
    {
        "_time": "2019-02-10T23:01:00Z",
        "exampleproperty0": "examplepropertyValue",
        "exampleproperty0_qc": "exampleproperty0_qc_Value",
        "exampleproperty1": "exampleproperty1Value"
    }
    ]"""
    bodyAsJson = json.dumps(dataList) 

    if logging in ("VERBOSE"):
        print("Trying to insert timeseries data with following body now ...")
        print(bodyAsJson)

    # Add some chunking since the TS API only supports 2000 datapoints 
    chunkSize = 2000
    listsWithTsData = [dataList[i:i + chunkSize] for i in range(0, len(dataList), chunkSize)]
    
    for listWithTsData in listsWithTsData:
        returnValue = wrapApiCall(f"/api/timeseries/{assetId}/{aspectName}" , "PUT", listWithTsData) 
        #Todo: Currently only the last result is being pased on.
    
    result = returnValue["result"]
    statusCode = returnValue["responseStatusCode"]
    responseText = returnValue["responseText"]
    

    returnDict = dict()
    returnDict["response"] = result
    returnDict["statusCode"] =statusCode
    returnDict["responseText"] = responseText
    
    return returnDict


############# OTHERS ##################

def getAgentStatus(asset):

    print(" °°°° Getting agent status'{}' now ... ".format(asset.name))
    bodyAsJson = {}

    
    returnValue = wrapApiCall("/api/agentmanagement/v3/agents/" + asset.agentData.agentId + "/status", "GET", bodyAsJson, assetTypeToDeriveApplicationScope=asset.typeId)
    result = returnValue["result"]
    statusCode = returnValue["responseStatusCode"]
    responseText = returnValue["responseText"]

    returnDict = dict()
    returnDict["response"] = result
    returnDict["statusCode"] =statusCode
    returnDict["responseText"] = responseText
    
    return returnDict


def offBoardAgent(asset):

    print(" °°°° Offboarding agent '{}' now ... ".format(asset.name))
    bodyAsJson = {}

    
    returnValue = wrapApiCall("/api/agentmanagement/v3/agents/" + asset.agentData.agentId + "/boarding/offboard", "POST", bodyAsJson, assetTypeToDeriveApplicationScope=asset.typeId)
    result = returnValue["result"]
    statusCode = returnValue["responseStatusCode"]
    responseText = returnValue["responseText"]

    returnDict = dict()
    returnDict["response"] = result
    returnDict["statusCode"] =statusCode
    returnDict["responseText"] = responseText
    
    return returnDict

#######################################

def uploadFile(file):
    # TODO: No idea if this is working ...doubt it
    print(" °°°° Uploading File '{}' now ... ".format(file.name))
    bodyAsJson = {}
    
    bodyAsJson["name"] = file.name
    bodyAsJson["type"] = file.typeOfFileInMindSphere
    bodyAsJson["parentId"] = file.directoryIdInMindSphere

    
    if logging == "VERBOSE":
        print("Trying to uploadFile with following body")
        print(bodyAsJson)

    fullFilePath = join(file.localDirectory, file.name)
    uploadFile =  {'file': open(fullFilePath, 'rb')}

    payload = {
    "metadata": json.dumps(bodyAsJson)
    }

    returnValue = wrapApiCall("/api/dataexchange/v3/files", "POST", data = payload, files = uploadFile, additionalHeaders = None)
    
    result = returnValue["result"]
    statusCode = returnValue["responseStatusCode"]
    responseText = returnValue["responseText"]

    returnDict = dict()
    returnDict["response"] = result
    returnDict["statusCode"] =statusCode
    returnDict["responseText"] = responseText
    
    return returnDict



