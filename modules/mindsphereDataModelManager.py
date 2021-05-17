import pdb
import uuid 
import helpers
import copy
import os
import json
from modules.helpers import SimpleError
from modules.mindsphereApiCollection import getAssetsFromMindSphere, getAssetTypesFromMindSphere, getAspectsFromMindSphere, getAgentsFromMindSphere, getAllDatapointMappingsForAgentAsset, getDeviceConfigurationForAgentAsset, getDatasourceConfigForAgentAsset
from modules.mindsphereApiCollection import createNewAssetInMindSphere, createNewAspectInMindSphere, createNewAssetTypeInMindSphere, getOnboardingKey, initializeAgentInMindSphere
from modules.datamodelClassDefinitions import Asset, AssetType, Aspect, ImportStatus, DataPointMapping, DeviceConfiguration, NetworkInterface, LibDataPoint, HardwareDataPoint, S7dataSource, OPCUAdataSource, LibDataSource

import modules.readConfig as config

tenantname = config.tenantname

# This modul should provide easy access to the mindsphere datamodel, so that you dont have to care about it
# All asset-related methods accept assetId or names.
# If an name is used, provide it as a keyword argument: name = <thename>
# It is encouraged to always use assetIds though, since identical names can and will occur multiple times in a tennant
from pprint import pprint

class MindsphereDataModelManager():

    tenantId = None
    randomLibAgentId = None
    randomNanoAgentId = None
    randomIot2040AgentId = None

    def __init__(self, fullMode = True): #-> This takes care of a lot of things and initializes the whole data model that is sitting in MindSphere

        self.tenantname = tenantname
        self.assetObjectsViaId = {}
        self.assetObjects = []

        self.assetTypeObjectsViaId = {}
        self.assetTypeObjects = []

        self.aspectObjectsViaId = {}
        self.aspectObjects = []

        self.agentAssetObjectsViaId = {}
        self.agentAssetObjects = []

  
        (simpleError,assetDictFromMindSphere) = getAssetsFromMindSphere()
        if simpleError.errorstatus:
            print(str(simpleError.errortext))

        #also provide access to the actual Asset-Objects in a hashed version
        for assetDictionary in assetDictFromMindSphere:
            currentAsset = Asset(
                name = assetDictionary["name"], 
                assetId = assetDictionary["assetId"], 
                description = assetDictionary["description"], 
                typeId = assetDictionary["typeId"],
                parentId = assetDictionary["parentId"],
                etag = assetDictionary["etag"]
            )

            self.assetObjects.append(currentAsset)
            self.assetObjectsViaId[currentAsset.assetId] = currentAsset

        #add RelevantParentInformation
        for asset in self.assetObjects:
            # parent = next((parent for parent in self.assetObjects if parent.assetId == asset.parentId), None)
            parent = self.getAssetViaId(asset.parentId)
            if parent:
                asset.parentName = parent.name
                asset.neutralParentId = parent.neutralAssetId
                parent.childIds.append(asset.assetId)

        if fullMode:            

            # Create Aspect-Objects
            (simpleError,aspectDictFromMindSphere) = getAspectsFromMindSphere()
            if simpleError.errorstatus:
                print(str(simpleError.errortext))
            
            for aspectDictionary in aspectDictFromMindSphere:
                currentAspect = Aspect(
                    name = aspectDictionary["name"], 
                    id = aspectDictionary["id"], 
                    description = aspectDictionary["description"],
                    etag = aspectDictionary["etag"]
                )
                for variable in aspectDictionary["variables"]:
                    name = variable["name"]
                    datatype  = variable["dataType"]
                    unit = variable["unit"]
                    currentAspect.addVariable(name, datatype, unit)
    
                self.aspectObjects.append(currentAspect)
                self.aspectObjectsViaId[currentAspect.id] = currentAspect

            # Create AssetType-Objects
            (simpleError,assetTypesDictFromMindSphere) = getAssetTypesFromMindSphere()
            if simpleError.errorstatus:
                print(str(simpleError.errortext))

            for assetTypeDictionary in assetTypesDictFromMindSphere:
                currentAssetType = AssetType(
                    name = assetTypeDictionary["name"], 
                    id = assetTypeDictionary["id"], 
                    description = assetTypeDictionary["description"], 
                    ancestorOfTypeId = assetTypeDictionary["parentTypeId"],
                    etag = assetTypeDictionary["etag"]
                    )
                
                for aspect in assetTypeDictionary["aspects"]:
                    aspectNameWithinAssetTypeContext = aspect["name"]
                    aspect = self.getAspect(aspect["aspectType"]["id"])
                    copyOfAspect = copy.copy(aspect)
                    copyOfAspect.aspectNameWithinAssetTypeContext = aspectNameWithinAssetTypeContext
                    currentAssetType.addAspect(copyOfAspect)
    
                self.assetTypeObjects.append(currentAssetType)
                self.assetTypeObjectsViaId[currentAssetType.id] = currentAssetType

            # Now acutally link references to the assetTypes to the asset object
            for asset in self.assetObjects:
                asset.linkToAssetType(self.getAssetType(asset.typeId))
                asset.ancestorOfTypeId = self.getAssetType(asset.typeId).ancestorOfTypeId

 

            # Create Agent-Objects
            (simpleError,agentsFromMindSphere) = getAgentsFromMindSphere()
            if simpleError.errorstatus:
                print(str(simpleError.errortext))

            for agentDictionary in agentsFromMindSphere:
                entityId = agentDictionary["entityId"]
                agentId = agentDictionary["id"]
                securityProfile = agentDictionary["securityProfile"]
                etag = agentDictionary["eTag"]
                relatedAsset = self.assetObjectsViaId[entityId]
                relatedAsset.initializeAgent(securityProfile,etag)

                self.agentAssetObjectsViaId[relatedAsset.assetId] = relatedAsset
                self.agentAssetObjects.append(relatedAsset)


        

        for agent in self.agentAssetObjects:
            if MindsphereDataModelManager.randomLibAgentId and MindsphereDataModelManager.randomNanoAgentId and MindsphereDataModelManager.randomIot2040AgentId:
                break
            if agent.typeId == "core.mclib":
                MindsphereDataModelManager.randomLibAgentId = agent.assetId
            if agent.typeId == "core.mcnano":
                MindsphereDataModelManager.randomNanoAgentId = agent.assetId
            if agent.typeId == "core.mciot2040":
                MindsphereDataModelManager.randomIot2040AgentId = agent.assetId

        MindsphereDataModelManager.tenantId = self.__lookupAssetIdforName(tenantname)

    #Asset Helpers
    def __lookupAssetForId(self, assetId):
        asset = self.assetObjectsViaId.get(assetId)
        
        if asset:
            return asset
        else:
            print(f"MINDSPHERE DATAMODEL ERROR: No name has been found for assetId {assetId}") 
            exit(-1)

    def __lookupNameForId(self, assetId):
        asset = self.assetObjectsViaId.get(assetId)
        
        if asset:
            return asset.name
        else:
            print(f"MINDSPHERE DATAMODEL ERROR: No name has been found for assetId {assetId}") 
            exit(-1)

    def __lookupAssetIdforName(self, name):
        listOfAssetIds = [x.assetId for x in self.assetObjects if x.name == name]
        if len(listOfAssetIds) == 1:
            return listOfAssetIds[0]

        elif len(listOfAssetIds) == 0:
            #print(f"AssetName '{name}' does not exist")
            return None

        else:
            print(f"MINDSPHERE DATAMODEL ERROR: Given name {name} is not unique in MindSphere and matches those {len(listOfAssetIds)} AssetIds {listOfAssetIds}") 
            exit(-1)


    def __assetTypeIdExists(self,assetTypeId):
        return self.assetTypeObjectsViaId.get(assetTypeId)


    def _decoratorResolvename(theFunction):
        def wrapper(self,*args, **kwargs):

            assetId = None
            name = None
            #print(kwargs)
            if args:
                assetId = args[0]
                args=() #Removing all positional arguments ...keep this in mind
            if 'assetId' in kwargs.keys():
                assetId = kwargs["assetId"]
            if 'name' in kwargs.keys():
                name = kwargs["name"]
         
            if assetId == None or assetId == False:
                assetId = self.__lookupAssetIdforName(name)
                kwargs["name"] = None
                kwargs["assetId"] = assetId

                return theFunction(self,*args, **kwargs)

            else:
                if name == None:
                    if self.assetObjectsViaId.get(assetId):
                        kwargs["name"] = None
                        kwargs["assetId"] = assetId

                        return theFunction(self,*args, **kwargs)
                    else:
                        print(f"AssetId '{assetId}' does not exist")
                        return None
                else: #in this case there is an assetId and an name
                    if name != self.__lookupNameForId(assetId):
                        print(f"MINDSPHERE DATAMODEL ERROR: Given name '{name}' does not fit the given assetId '{assetId}' ")
                        exit(-1)
                    else:
                        kwargs["name"] = None
                        kwargs["assetId"] = assetId
                        return theFunction(self,*args, **kwargs)
                     
            return theFunction(*args, **kwargs)
        return wrapper


# AssetType Helpers:

#### External Methods:

#ASSETS:
    @_decoratorResolvename
    def isAgent(self, assetId = None, name = None):
        pass
    
    @_decoratorResolvename
    def getParent(self, assetId = None, name = None):
        pass

    @_decoratorResolvename
    def getChildren(self,assetId = None, name = None):
        pass

    @_decoratorResolvename
    def findFittingAspect(self,assetId = None, name = None):
        pass

    @_decoratorResolvename
    def getAsset(self, assetId = None, name = None):
        if not assetId: #If there is still no assetId after the preprocesser was running, the object does not exist in MindSphere
            return None
        return self.assetObjectsViaId.get(assetId) # the get dictionary-method will return None if the key is not existing

    def getAssetViaId(self, assetId):
         return self.assetObjectsViaId.get(assetId) # the get dictionary-method will return None if the key is not existing


    def _recursiveChildrenLookup(self,parentsOnCurrentTreeLevel):
        #returns list in this order: [all child of childs...all childs...parent]
        #In other words the grand-children will be first in the list
        #Also this will populate attributes that require inheritance (offboard agents, deleteUnderlyingDatamodel ...)

        childTreeLayer=[]
        for currentParentAsset in parentsOnCurrentTreeLevel:
            directChilds = [x for x in self.assetObjects if x.parentId == currentParentAsset.assetId]
            for child in directChilds:
                child.parentName = currentParentAsset.name
                child.deleteUnderlyingChilds = currentParentAsset.deleteUnderlyingChilds
                child.deleteUnderlyingDatamodel = currentParentAsset.deleteUnderlyingChilds
                child.offboardAgents = currentParentAsset.offboardAgents
                currentParentAsset.directlyRelatedChilds.append(child)
                childTreeLayer.append(child)

        if childTreeLayer:
            return self._recursiveChildrenLookup(childTreeLayer) + parentsOnCurrentTreeLevel 
        else: 
            return parentsOnCurrentTreeLevel


    def getListWithAllChildren(self, currentAsset, parentsFirst = False):
        if currentAsset.completeChildrenTree: #Maybe this has been created before already
            return currentAsset.completeChildrenTree[::-1] if parentsFirst else currentAsset.completeChildrenTree

        else:
            print("Getting complete children tree for parent '{}' with id '{}' now ...".format(currentAsset.name, currentAsset.assetId))
            childrenTree = self._recursiveChildrenLookup([currentAsset])
            currentAsset.completeChildrenTree = childrenTree
            return currentAsset.completeChildrenTree[::-1] if parentsFirst else currentAsset.completeChildrenTree


    def getMultipleAssetsViaName(self, name):
        return [asset for asset in self.assetObjects if asset.name == name]

    def getAllEndNodeChildren(self):
        return [asset for asset in self.assetObjects if not asset.childIds]

    def getAllEndNodeChildrenThatAreNotAgents(self):
        return [asset for asset in self.assetObjects if not asset.childIds and asset.agentData == None]

    def getAllEndNodeChildrenThatAreAgents(self):
        return [asset for asset in self.assetObjects if not asset.childIds and asset.agentData != None]

    def getAllAgents(self):
        return [asset for asset in self.assetObjects if asset.agentData != None]

    def getAllAssetTypes(self):
        return [assetType for assetType in self.assetTypeObjects]

    def getAllAspects(self):
        return [aspect for aspect in self.aspectObjects]


    def getAssetViaNameAndParent(self,name, parentId):

        allAssetsWithFittingName = self.getMultipleAssetsViaName(name)
        fittingAssets = []
        for asset in allAssetsWithFittingName:
            if asset.parentId == parentId:
                fittingAssets.append(asset)
        return fittingAssets

    def getAssetsIncludingUserDecision(self, assetNameOrId, parentId = None, allowToChooseNone = False, allowMultipleSelections = False, allowToEnterOwnId = False):
        # Will always a list of assets (even if only one asset is inside the list)
        
        returnList = []
        couldBeAnId = False
        try:
            uuid.UUID(assetNameOrId) 
            if len(assetNameOrId) > 15:
                couldBeAnId = True
            
        except ValueError:
            pass
        
        if couldBeAnId:
            assetViaId = self.getAsset(assetId = assetNameOrId)
            if assetViaId:
                returnList.append(assetViaId)
                return returnList

        #Detection via ID failed - now treat the identifier as a name:
        if parentId:
            assetsInFocus= self.getAssetViaNameAndParent(assetNameOrId, parentId = parentId)
        else:
            assetsInFocus = self.getMultipleAssetsViaName(assetNameOrId)
        
        originalLength = len(assetsInFocus)

        if not assetsInFocus:
            if allowToEnterOwnId:
                assetNameOrId = input(f"\nA search for an asset with name '{assetNameOrId}' resulted in no assets within the MindSphereDataModel. Do you want to add a manual id yourself?")
                assetsInFocus = self.getMultipleAssetsViaName(assetNameOrId)

        if not assetsInFocus: #Nothing has been found
            return returnList

        if allowToChooseNone and not allowMultipleSelections:
            assetsInFocus.append(None)

        if len(assetsInFocus) == 1:
            return assetsInFocus
        else:
            if originalLength > 1:
                print(f"\nA search for an asset with name '{assetNameOrId}' resulted in multiple assets within the MindSphereDataModel")
            else: 
                print(f"\nSearching for an asset with the identifier '{assetNameOrId}' resulted in one match within the MindSphereDataModel")
            if allowMultipleSelections:
                print("Please choose all, that should be used here and enter the corresponding Nos in a comma separated list. Enter 'all' for all")            
            else:
                print("Please choose which one should be taken over for the following process (and enter the corresponding No.)")

            if allowToChooseNone and not allowMultipleSelections:
                print("If you choose the None-Asset, the Data Model Handler will act as if this asset would not exist. You would do this e.g. if you want to use a new asset with an identical name")
            
            print("_"*159)
            print(f"{'No.':<5}|{'Assetname':^35}|{'AssetId':^40}|{'ParentName':^35}|{'ParentId':^40}")
            print("*"*159)
            for idx, asset in enumerate(assetsInFocus):
                if asset:
                    parentAsset = self.__lookupAssetForId(asset.parentId)
                    print(f"{idx+1:<5}|{asset.name:^35}|{asset.assetId:^40}|{parentAsset.name:^35}|{asset.parentId:^40}")
                else:
                    print(f"{idx+1:<5}|{'None':^35}|{'None':^40}|{'None':^35}|{'None':^40}")

            decisionMissing = True

            while(decisionMissing):
                if allowMultipleSelections:
                    providedInput = input("\nPlease speficy all assets to be used in a comma-separated listed of the No.s:\n")
                    if providedInput.lower().strip() =='all':
                        return assetsInFocus
                    else:
                        indexList= providedInput.split(",")
                        returnList = [assetsInFocus[int(x.strip())-1] for x in indexList]
                        return returnList
                else:
                    providedInput = input("\nPlease speficy a No. for the asset to be used here:\n")                
                    try:
                        providedInput = int(providedInput)
                        if providedInput > 0 and providedInput<= len(assetsInFocus) :
                            chosenAsset = assetsInFocus[providedInput-1]
                            if chosenAsset!= None:
                                returnList.append(chosenAsset)
                            return returnList
                        else:
                            print("Your input '{}' does not match the allowed values".format(providedInput))
                    except Exception as e:                        
                        print(e)

#AGENTS
    def collectDataSourcesForAgent(self, asset):
        if not asset.agentData:
            print("This asset '{asset.name}' is not a supported agent - no DataSources will be collected")
            return None
            
        # Now fetch the DataSourceConfig
        (simpleError,datasourceConfiguration) = getDatasourceConfigForAgentAsset(asset)
        if simpleError.errorstatus:
            print(str(simpleError.errortext))
        if not datasourceConfiguration:
            return None

        # Momentan wird einfach das ganze Konstrukt as-is als List of Dictionary abgelegt
        # TODO: Hier könnte man die Inhalte noch nach den einzelnen Datasources aufsplitten und auch die Datapoint-ID anyonymisieren/neutralisieren - muss man aber wohl nicht
        # Beim Import muss man allerdings  später aufpassen, dass die neuen IDs korrekt zueinander aufgebaut werden.
        
        # Das Format was momentan hier gespeichert wird schaut so aus:
        # Für McLIB:
        """[{'id': '87359fee-1500-48a2-86e3-42d9dfbc89e9', 
        'configurationId': '1562236765291', 
        'dataSources': [{'name': 'DataSource1', 'customData': None, 'description': '', 'dataPoints': [{'id': '1562236719938', 'name': '100000000', 'description': '', 'unit': 'C', 'type': 'DOUBLE', 'customData': None}, {'id': '1562236748772', 'name': '100000001', 'description': '', 'unit': 'g/m³', 'type': 'DOUBLE', 'customData': None}]}, {'name': 'Test23', 'customData': None, 'description': '', 'dataPoints': []}], 'eTag': '2'}]"""
        
        # Für McNano/IoT2040 (hier gibt es noch den übergeordneten uploadCycle parameter, der für die ganze Box und alle definierten Datasources gilt)
        """[{'agentId': '7de1390a23d744a893502b84290a4b6f', 
        uploadCycle': '10', 
        'configurationId': '1571734761412', 
        'dataSources': [{'name': 'MyDataSourceS7Name', 'description': 'MyDataSourceS7Desc', 'protocol': 'S7', 'readCycleInSeconds': '60', 'protocolData': {'ipAddress': '128.25.25.25'}, 'dataPoints': [{'dataPointId': 'b955e6c761e04', 'name': 'MyDataPointS7', 'description': 'MyDataPointS7Desc', 'unit': 'MyCrazyUnit', 'dataType': 'DOUBLE', 'dataPointData': {'address': 'DB15.DBX13.4', 'onDataChanged': False}}]}, {'name': 'MyOPCUAName', 'description': 'MyOPCUADesc', 'protocol': 'OPCUA', 'readCycleInSeconds': '60', 'protocolData': {'opcUaServerName': 'MyOPCUAServer', 'opcUaServerAddress': 'opc.tcp://128.42.5.2', 'opcUaServerIPAddress': '128.42.5.2', 'opcUaCertificateMetadata': '', 'opcUaAuthenticationType': 'BASIC', 'opcUaUsername': 'HorstUsername', 'opcUaPassword': 'HorstPassword'}, 'dataPoints': [{'dataPointId': 'e1b9c7fd16f44', 'name': 'MyOPCAUADataPoint', 'description': '', 'unit': 'OPCUAUNIT', 'dataType': 'DOUBLE', 'dataPointData': {'address': 'ns=Horst,s=myNode', 'onDataChanged': False}}]}]}]"""

        
            
        if asset.typeId == "core.mclib":
            print(f"Getting DatasourceConfigurations for agent '{asset.name}'...")
            for dataSource in datasourceConfiguration["dataSources"]: 
                currentDataSource = LibDataSource(
                        name = dataSource["name"],
                        agentReference = asset.agentData,
                        description = dataSource["description"],
                        customData = dataSource["customData"]
                        )
            #[{'id': '1562236719938', 'name': '100000000', 'description': '', 'unit': 'C', 'type': 'DOUBLE', 'customData': None}
                for dataPoint in dataSource["dataPoints"]:
                    print(dataPoint)
                    currentDataPoint = LibDataPoint(
                        dataPointId = dataPoint["id"],
                        name = dataPoint["name"],
                        unit = dataPoint["unit"],  
                        dataType = dataPoint["type"],
                        variable= dataPoint["name"],
                        customData = dataPoint["customData"],
                        description = dataPoint["description"]
                    )
                    currentDataSource.addDataPoint(currentDataPoint)

                asset.agentData.addDataSource(currentDataSource)

        else: # for nanao and iot240
 
            asset.agentData.uploadCycle = datasourceConfiguration["uploadCycle"]
            for dataSource in datasourceConfiguration["dataSources"]: 
                if dataSource["protocol"] == "S7":
                    currentDataSource = S7dataSource(
                        id = dataSource["id"], 
                        name = dataSource["name"], 
                        agentReference = asset.agentData,
                        protocol = dataSource["protocol"],
                        readCycleInSeconds = dataSource["readCycleInSeconds"], 
                        ipAddress = dataSource["protocolData"]["ipAddress"],
                        manualRackAndSlot = dataSource["protocolData"].get("manualRackAndSlot"),
                        rackNumber = dataSource["protocolData"].get("rackNumber"),
                        slotNumber =dataSource["protocolData"].get("slotNumber"),
                        description =dataSource["description"]
                    )
                elif dataSource["protocol"] == "OPCUA":
                    currentDataSource = OPCUAdataSource(
                        id = dataSource["id"], 
                        name = dataSource["name"], 
                        agentReference = asset.agentData,
                        protocol = dataSource["protocol"],
                        readCycleInSeconds = dataSource["readCycleInSeconds"], 
                        opcUaServerName = dataSource["protocolData"]["opcUaServerName"],
                        opcUaServerAddress =dataSource["protocolData"]["opcUaServerAddress"],
                        opcUaServerIPAddress = dataSource["protocolData"]["opcUaServerIPAddress"], 
                        description =dataSource["description"]
                    )
                
                else:  #TODO: Hier ggfs den DataSource-Type "System" hinzufügen
                    continue

                for dataPoint in dataSource["dataPoints"]:
                    
                    if "acquisitionType" in dataPoint["dataPointData"]: # This attribut is only existing for devices with current firmware
                        acquisitionType = dataPoint["dataPointData"]["acquisitionType"]
                    else: 
                        acquisitionType = "READ"

                    if "hysteresis" in dataPoint["dataPointData"]: 
                        hysteresis = dataPoint["dataPointData"]["hysteresis"]
                    else: 
                        hysteresis = 0

                    if "onDataChanged" in dataPoint["dataPointData"]: 
                        onDataChanged = dataPoint["dataPointData"]["onDataChanged"]
                    else: 
                        onDataChanged = False


                    currentDataPoint = HardwareDataPoint(
                        dataPointId=dataPoint["dataPointId"],
                        name=dataPoint["name"],
                        unit=dataPoint["unit"],
                        variable=dataPoint["name"],
                        dataType=dataPoint["dataType"],
                        description=dataPoint["description"],

                        address=dataPoint["dataPointData"]["address"],
                        onDataChanged=onDataChanged,
                        hysteresis = hysteresis,
                        acquisitionType = acquisitionType
                        )
                
                    currentDataSource.addDataPoint(currentDataPoint)

                asset.agentData.addDataSource(currentDataSource)

        return asset.agentData.dataSources

    def collectDeviceConfigForAgent(self, asset):

        if not asset.agentData:
            print("This asset '{asset.name}' is not a supported agent - no Device Configuration will be collected")
            return None


        if not asset.agentData.deviceConfiguration: #Only do that, if it has not been done before yet
            print(f"Getting Device Configuration for agent '{asset.name}'...")
            (simpleError,deviceConfig) = getDeviceConfigurationForAgentAsset(asset)
            if simpleError.errorstatus:
                print(str(simpleError.errortext))
            if deviceConfig: #This should be the case, if it is not an mc.lib item
                currentDeviceConfiguration = DeviceConfiguration(
                serialNumber = deviceConfig["device"]["serialNumber"],
                deviceType = deviceConfig["device"]["deviceType"],
                boardingStatus = deviceConfig["boardingStatus"],
                deviceIdentifier = deviceConfig["device"]["deviceIdentifier"]
                )
                for networkInterface in deviceConfig["device"]["networkInterfaces"]:
                    if networkInterface["DHCP"]["enabled"] == True:
                        currentNetworkInterface = NetworkInterface(
                            name = networkInterface["name"],
                            DHCP = True
                        )
                    else:  
                        currentNetworkInterface = NetworkInterface(
                            name = networkInterface["name"],
                            DHCP = False,
                            IPv4 = networkInterface["static"]["IPv4"],
                            IPv6 = networkInterface["static"]["IPv6"],
                            DNS = networkInterface["static"]["DNS"],
                            gateway = networkInterface["static"]["Gateway"],
                            subnetMask = networkInterface["static"]["SubnetMask"]
                        )
                    currentDeviceConfiguration.addNetworkInterface(currentNetworkInterface)

                asset.agentData.assignDeviceConfig(currentDeviceConfiguration)
                return currentDeviceConfiguration

    def collectValidDataPointMappingForAgent(self, asset):

        if not asset.agentData:
            print("This asset '{asset.name}' is not a supported agent - no datapointMappings will be collected")
            return None
       
        if not asset.agentData.dataPointMappingsHaveBeenCollectedFromMindSphere: #Only do that, if it has not been done before yet
            print(f"Getting Datapointmappings for agent '{asset.name}'...")
            (simpleError,dataPointMappings) = getAllDatapointMappingsForAgentAsset(asset)
            if simpleError.errorstatus:
                print(str(simpleError.errortext))
            
            listWithFreeFloatingMappings = []
            for dataPointMappingDict in dataPointMappings:
                if dataPointMappingDict['validity']['status'] == 'VALID': #Only get valid mappings, ignore all others
                    currentDataPointMapping = DataPointMapping(
                    agentId = dataPointMappingDict["agentId"], 
                    targetAsset =  self.getAssetViaId(dataPointMappingDict["entityId"]), 
                    dataPointId = dataPointMappingDict["dataPointId"],
                    aspectId = dataPointMappingDict["propertySetName"],
                    variableName =dataPointMappingDict["propertyName"]
                    )
                    listWithFreeFloatingMappings.append(currentDataPointMapping)
            #Now assign the data to the different Datasources.
            for dataSource in asset.agentData.dataSources:
                for dataPoint in dataSource.dataPoints:
                    relatedDataPointMapping = next((dataPointMapping for dataPointMapping in listWithFreeFloatingMappings if dataPoint.dataPointId == dataPointMapping.dataPointId),None)
                    if relatedDataPointMapping:
                        dataPoint.addDatapointMapping(relatedDataPointMapping)
            asset.agentData.dataPointMappingsHaveBeenCollectedFromMindSphere = True
        return True

#ASSET-TYPES:
    def getAssetType(self, nameOrId):        

        assetTypeId = helpers.deriveIdFromNameOrId(nameOrId)
        return self.assetTypeObjectsViaId.get(assetTypeId) # the get dictionary-method will return None if the key is not existing

    
#ASPECTS:
    def getAspect(self, nameOrId):        
        aspectId = helpers.deriveIdFromNameOrId(nameOrId)
        return self.aspectObjectsViaId.get(aspectId) # the get dictionary-method will return None if the key is not existing



######################


def importAssetsWithoutCheckingAnything(assetsToBeImported,mindsphereDataModelManager):

    successedAssets = []
    failedAssets = []


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
                continue

        returnDict = createNewAssetInMindSphere(asset)
        statusCode = int(returnDict["statusCode"])
        response = returnDict["response"]
        responseText = returnDict["responseText"]

        if statusCode > 200 and statusCode <300:
            successedAssets.append(asset)
            asset.setAssetImportSucceeded(returnDict)
            asset.alreadyExistingInMindSphere = True

        else:
            asset.error.addHardError("Asset Import failed, response text was: {}".format(responseText))
            failedAssets.append(asset)
            asset.setAssetImportFailed(returnDict)

#####################

def importAssetTypesWithoutCheckingMuch(assetTypesToBeImported,mindsphereDataModelManager):
    #Todo: Refactor to use importer mechanisms, check if already existing and stuff like that
    successedAssetTypes= []
    successedAspects = []
    failedAssetTypes= []
    failedAspects =[]

    for assetType in assetTypesToBeImported:
        assetTypeInMindSphere = mindsphereDataModelManager.getAssetType(assetType.id)
        if assetTypeInMindSphere:
            print(f"Asset-Type '{assetType.id}' already existing in Mindsphere - it will not be imported")
            #todo: add "extend asset-type" here

        for aspect in assetType.getAspectsThatNeedImporting(): 

            aspectAlreadyImported = next((x for x in successedAspects if x.id == aspect.id), None)
            if aspectAlreadyImported:
                aspect.AspectImportStatus = ImportStatus.NOT_NECESSARY
                #No need to import this aspect, someone else did it before.
                #Continute with next aspect
                continue
     
            aspectAlreadyFailed = next((x for x in failedAspects if x.id == aspect.id), None)
            if aspectAlreadyFailed:
                #Abort this asset, since the aspect creation failed already before
                print("Import of Aspect '{}' failed already in a previous atempt, asset wont be imported".format(aspect.name))
                continue

           
            returnDict = createNewAspectInMindSphere(aspect)
            statusCode = int(returnDict["statusCode"])
            response = returnDict["response"]
            responseText = returnDict["responseText"]

            if statusCode >= 200 and statusCode <300:
                successedAspects.append(aspect)
                aspect.setAspectImportSucceeded(returnDict)
            else:
                print("Aspect Import failed, response text was: {}".format(responseText))
                failedAspects.append(aspect)
                aspect.setAspectImportFailed(returnDict)


        # Import ASSET TYPES NOW
        assetTypeAlreadyImported = next((x for x in successedAssetTypes if x.id == assetType.id), None) # and x["parentTypeId"] == asset.assetTypeToBeImported.parentAssetTypeId)  -> this additional check is not useful, since aspectNames are unique in MindSphere
        if assetTypeAlreadyImported:
            continue

        else:
            #Only Import something, if it has not already been tried to import it before ....

            assetTypeAlreadyFailed = next((x for x in failedAssetTypes if x.id == assetType.id), None)
            if assetTypeAlreadyFailed:
                #Abort this asset, since the aspect creation failed already before
                print("Import of AssetType '{}' failed already in a previous atempt, asset wont be imported".format(assetType.id))
                #Continue with next asset
                continue
        
            #Everthing is looking fine so far, starting importing of assetType now
            
            returnDict = createNewAssetTypeInMindSphere(assetType)
            statusCode = int(returnDict["statusCode"])
            response = returnDict["response"]
            responseText = returnDict["responseText"]

            if statusCode > 200 and statusCode <300:
                successedAssetTypes.append(assetType)
                assetType.setAssetTypeImportSucceeded(returnDict)
            else:
                print("AssetType Import failed, response text was: {}".format(responseText))
                failedAssetTypes.append(assetType)
                assetType.setAssetTypeImportFailed(returnDict)
   

######################

def createAndOnboardAgents(agentAssetsToBeCreatedList):

    successedAssets = []
    failedAssets = []

    for asset in agentAssetsToBeCreatedList:

        # First import the agent
        if asset.error.hardError == True:
            continue #Skip Items with error and Items alread in MindSphere
        if not asset.alreadyExistingInMindSphere: # asset could have already been imported before
            if asset.parentId == None: #Now check, if parentName has already been imported during the current import process (check if parentName is in "successfully imported list")

                parent = next((x for x in successedAssets if x.name == asset.parentAssetNameOrId and x.neutralAssetId == asset.neutralParentId), None)
                if parent and parent.name:
                    asset.parentId = parent.assetId

                else: #No parentName available - import will definitely fail
                    asset.error.addHardError(f"Import of Agent-Asset '{asset.name}' cannot be performed because it's parent seems to be missing")
                    asset.assetImportStatus = ImportStatus.FAILED #Set general Asset-Status to failed
                    continue

            returnDict = createNewAssetInMindSphere(asset)
            statusCode = int(returnDict["statusCode"])
            response = returnDict["response"]
            responseText = returnDict["responseText"]

            if statusCode > 200 and statusCode <300:
                successedAssets.append(asset)
                asset.setAssetImportSucceeded(returnDict)
                asset.agentData.agentId = asset.assetId
            else:
                asset.error.addHardError("Asset Import failed, response text was: {}".format(responseText))
                failedAssets.append(asset)
                asset.setAssetImportFailed(returnDict)
                continue
        
        # Then setup the agent itself
        initializeAgentInMindSphere(asset)

        # Then get the onboardingKey and save it to a file

        onboardingKey = getOnboardingKey(asset)["response"]
        if asset.typeId != "core.mclib":        
            serialNumber = asset.agentData.deviceConfiguration.serialNumber
            filename = f"ConBox_{serialNumber}_Config.cfg"
        
        else:
            filename = f"onboardingKey_LibAgent_{asset.name}.txt"

        onboardingKeyFile = os.path.join(asset.agentData.rootPathOfDefinitionFile, filename)
        
        with open(onboardingKeyFile, 'w') as keyfile:
            keyfile.write(json.dumps(onboardingKey))
            

######################

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
                print("Aspect to be imported:")
                pprint(vars(aspect))
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

        #print(str(successedAssets))

    print("All asset imports have been completed or at least attempted")