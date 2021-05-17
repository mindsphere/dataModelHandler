from enum import Enum
import pdb
import time
#Custom Modules
from modules.helpers import SimpleError
import modules.readConfig as config
from itertools import count


#######################################
######### HELPER FUNCTIONS ############
#######################################
neutralIdRepository = {}
neutralIdCounter = count(1)

def lookUpNeutralId(originalId = None):
    if originalId:

        if config.convertToNeutralIds:

            if originalId in neutralIdRepository:
                return neutralIdRepository[originalId]
            else:
                nextId = next(neutralIdCounter)
                neutralIdRepository[originalId] = nextId
                return nextId
        else:
            return originalId
    else:
        if neutralIdRepository:
            print("Printing Device-ID Mapping dictionary for Debug-Reasons now:")
            for entry in neutralIdRepository:
                print("{} -> {}".format(entry,neutralIdRepository[entry]))


class ImportStatus(Enum):

    NOT_NECESSARY = 1
    SUCCESSFUL = 2
    FAILED = 3

class File():
    def __init__(self, name, directory = None):
        self.name = name
        self.parentDirectoryId = None
        self.localDirectory = directory
        self.typeOfFileInMindSphere = None
        self.directoryIdInMindSphere = None

    def setParentDirectoryId(self,parentDirectoryId):
        self.parentDirectoryId = parentDirectoryId
    
    def output(self):
        print("\nFilename: '{}'".format(self.name), end ='') 
        if self.localDirectory:
            print("\n\t\t\t...from Directory: '{}'".format(self.localDirectory), end ='')
        print("")


class Variable():

    def __init__(self,name,dataType, unit,flowMin, flowMax, flowLikelihood):
        if " " in name:
            print(f"Replacing spaces in variable name '{name}' with _ ")
            name = name.replace(" ","_")
        if "." in name:
            print(f"Replacing . in variable name '{name}' with _ ")
            name = name.replace(".","_")
        if "-" in name:
            print(f"Replacing - in variable name '{name}' with _ ")
            name = name.replace("-","_")

        self.name = name
        self.dataType = dataType
        self.unit = unit
        
        # Related to VFC simulation:
        self.flowMin = flowMin
        self.flowMax = flowMax
        self.flowLikelihood = flowLikelihood


class Aspect():

    importNotNecessaryList = []
    successfullyImported = []
    toBeImportedList = []
    failedList= []
                
    def __init__(self, name, id, description = None, aspectNameWithinAssetTypeContext = None, etag = 0,category='dynamic',scope ='private' ):
        self.id = id

        self.name = name
        self.description = description
        self.variables = []
        self.AspectImportStatus = None # NOT NECESSARY, FAILED, SUCCESSFUL
        self.AspectImportStatusResponse = None
        self.ancestorOfTypeId = None
        self.relatedLines = []
        self.error = SimpleError()
        self.importResponse = None
        self.idAfterSuccessfulImport = None
        self.etag = etag
        #self.aspects = []
        self.relatedToAssetTypeIds = []
        self.aspectNameWithinAssetTypeContext = aspectNameWithinAssetTypeContext
        self.category = category
        self.scope = scope

    def __eq__(self, other):
        return self.id==other.id

    def __hash__(self):
        return hash(('id', self.id))

    def setNameWithinAssetTypeContext(self, aspectNameWithinAssetTypeContext):
        self.aspectNameWithinAssetTypeContext = aspectNameWithinAssetTypeContext

    def addVariable(self,variablename, dataType, unit, flowMin = None, flowMax = None, flowLikelihood = None):
        newVariable = Variable(variablename,dataType,unit,flowMin, flowMax, flowLikelihood)
        self.variables.append(newVariable)

    def setAspectImportSucceeded(self,returnDict):
        self.AspectImportStatus = ImportStatus.SUCCESSFUL
        self.AspectImportStatusResponse = returnDict

    def setAspectImportFailed(self,returnDict):
        self.AspectImportStatus = ImportStatus.FAILED
        self.AspectImportStatusResponse = returnDict

    #def addAspect(self,aspect):
        # self.aspects.append(aspect)
    
    def getOutputDictionaryInAssetTypeStyle(self):
        # This is not needed...
        outputDict = {"name": self.aspectNameWithinAssetTypeContext,"aspectType": {
                "id": self.id,
  
                "name": self.name,
                "category": self.category,
                "scope": self.scope,
                "description": self.description, 
                "variables":[vars(variableDefintion) for variableDefintion in self.getVariables()]}}

        return outputDict
        

    def getVariables(self):
        return self.variables

    def simpleOutput(self):
        print("{}@{}".format(self.name, self.id))
        
    def output(self):
        print("-" * 20)
        print("AspectName: {}".format(self.name))
        print("RelatedLines in Input File: {}".format(str(self.relatedLines)))
        print("Aspect Description: {}".format(self.description)) 

        if self.error.errorstatus:
            if self.error.hardError:
                print("HARD ERROR: {}, Message: {}".format(self.error.errorstatus, str(self.error.errortext)))
            else:
                print("Soft error: {}, Message: {}".format(self.error.errorstatus, str(self.error.errortext)))

    def setAspectDeletionFailed(self,returnDict):
        self.AspectDeletionStatus = ImportStatus.FAILED
        self.AspectDeletionStatusResponse = returnDict

    def setAspectDeletionSucceeded(self,returnDict):
        self.AspectDeletionStatus = ImportStatus.SUCCESSFUL
        self.AspectDeletionStatusResponse = returnDict



class AssetType():
    
    importNotNecessaryList = []
    successfullyImported = []
    toBeImportedList = []
    failedList= []

    def __init__(self, id, name, description = None, ancestorOfTypeId = None,etag = 0 ):
        
        self.id = id
        self.name = name
        self.description = description
        self.ancestorOfTypeId = ancestorOfTypeId
        self.assetTypeDescription  = description
        self.aspects = []
        self.aspectsThatNeedImporting = []
        self.assetTypeImportStatus = None # NOT NECESSARY, FAILED, SUCCESSFUL
        self.assetTypeImportStatusResponse = None
        self.importResponse = None
        self.relatedLines = []
        self.error = SimpleError()
        self.idAfterSuccessfulImport = None
        self.deleteUnderlyingChilds = None
        self.deleteUnderlyingDatamodel = None
        self.relatedToAssetIds = []
        self.etag = etag

    def __eq__(self, other):
        return self.id==other.id

    def __hash__(self):
        return hash(('id', self.id))



    def setAssetTypeImportSucceeded(self,returnDict):
        self.assetTypeImportStatus = ImportStatus.SUCCESSFUL
        self.assetTypetStatusResponse = returnDict

    def setAssetTypeImportFailed(self,returnDict):
        self.assetTypeImportStatus = ImportStatus.FAILED
        self.assetTypeImportStatusResponse = returnDict

    def output(self):
        print("-" * 20)
        print("AssetTypeName: {}".format(self.name))
        print("RelatedLines in Input File: {}".format(str(self.relatedLines)))
        print("AssetType Description: {}".format(self.description)) 
        print("Ancestor of AssetType Id: {}".format(self.ancestorOfTypeId)) 

        if self.error.errorstatus:
            if self.error.hardError:
                print("HARD ERROR: {}, Message: {}".format(self.error.errorstatus, str(self.error.errortext)))
            else:
                print("Soft error: {}, Message: {}".format(self.error.errorstatus, str(self.error.errortext)))
    
    def simpleOutput(self):
        print("{}@{}".format(self.name, self.id))

    def setAssetTypeDeletionFailed(self,returnDict):
        self.assetTypeDeletionStatus = ImportStatus.FAILED
        self.assetTypeDeletionStatusResponse = returnDict

    def setAssetTypeDeletionSucceeded(self,returnDict):
        self.assetTypeDeletionStatus = ImportStatus.SUCCESSFUL
        self.assetTypeDeletionStatusResponse = returnDict


    def addAspect(self, aspect):
        self.aspects.append(aspect)

    def addAspectThatNeedsImporting(self,aspect):
        self.aspectsThatNeedImporting.append(aspect)

    def getAspects(self):
        return self.aspects
    
    def getAspectsThatNeedImporting(self):
        return self.aspectsThatNeedImporting


class NetworkInterface():
    def __init__(self, name, DHCP = True, IPv4 = None, subnetMask = None, gateway = None, DNS = None, IPv6 = None):

        self.name = name
        self.DHCP = DHCP
        
        if DHCP==True:
            self.IPv4 = None
            self.subnetMask = None
            self.gateway = None
            self.IPv6 = None
            self.DNS = DNS

        else:
            self.IPv4 = IPv4
            self.subnetMask = subnetMask
            self.gateway = gateway
            self.IPv6 = IPv6
            self.DNS = DNS


class DeviceConfiguration():
    def __init__(self, deviceType, serialNumber, boardingStatus = None, deviceIdentifier = None):
        self.networkInterfaces = []
        self.deviceType = deviceType
        self.serialNumber = serialNumber
        self.boardingStatus = boardingStatus
        self.networkInterfaces = []
        self.deviceIdentifier =  deviceIdentifier # braucht man das ding?

    def addNetworkInterface(self,networkinterface):
        if not next((x for x in self.networkInterfaces if x.name == networkinterface.name), None):
            self.networkInterfaces.append(networkinterface)
            return True
        else:
            print(f"{networkinterface.name} already defined on this agent")
            return False
    def getNetworkInterface(self, name):
        return next((x for x in self.networkInterfaces if x.name == name), None)



class DataPointMapping():

    def __init__(self,  targetAsset, dataPointId, aspectId, variableName, agentId = None, agentAsset = None):
        self.agentId = agentId
        self.agentAsset = agentAsset
        self.neutralAgentId = agentId
        self.targetAsset = targetAsset
        self.dataPointId = dataPointId
        self.aspectId = aspectId
        if " " in variableName:
            print(f"Replacing spaces in variable name '{variableName}' with _ ")
            variableName = variableName.replace(" ","_")
        if "." in variableName:
            print(f"Replacing . in variable name '{variableName}' with _ ")
            variableName = variableName.replace(".","_")
        if "-" in variableName:
            print(f"Replacing - in variable name '{variableName}' with _ ")
            variableName = variableName.replace("-","_")
        self.variableName =  variableName

        self.mappingMode = None #Used in the Agent Importer context
        self.mappedTargetAssetDictionary = {}
        self.targetAssetCreationNecessary = None

class DataSource():
    def __init__(self, name, agentReference, protocol,description = '',):

        self.agentReference = agentReference
        self.name = name
        self.description = description
        self.protocol = protocol
        self.dataPoints = []
        self.dataPointsFileName = name + "_datapoints.csv"

        self.mappingMode = None #Used in the Agent Importer context
        self.mappedTargetAssetDictionary = {}
        

    def addDataPoint(self, datapoint):
        self.dataPoints.append(datapoint)

    def getDictListOutputForDataPointsAndMappings(self, basicDict):
            # basicDict is an empty dict where the keys are column names
            # This is needed, that every column will be in the output file (even if it is an empty column)
            workingDictList = []
            
            for dataPoint in self.dataPoints:
                workingDict = basicDict.copy()
                #General stuff related to Datapoint
                workingDict["Datapoint and Variable Name"] = dataPoint.name
                workingDict["DataPoint Id"] = dataPoint.dataPointId            
                workingDict["Description"] = dataPoint.description
                workingDict["Unit"] = dataPoint.unit
                workingDict["DataType"] = dataPoint.dataType

                if self.protocol != "LIB":
                    #S7 / OPCUA only
                    workingDict["Address"] = dataPoint.address
                    workingDict["On Data Changed"] = dataPoint.onDataChanged
                    workingDict["Hysteresis"] = dataPoint.hysteresis

                if self.protocol == "S7":

                    workingDict["Acquisition Type (S7 only)"] = dataPoint.acquisitionType 
                if dataPoint.dataPointMappings:
                    for dataPointMapping in dataPoint.dataPointMappings:

                        basicDictwithDataPointInformation = workingDict.copy()
                        basicDictwithDataPointInformation["mappingMode"] = config.defaultMappingModes #Todo: Think about if this should rather be set to "none" (since the mapping modeis already specified on the datasource level) or if there should be another config parameter to define the defaults on datapoint mapping
                        if config.exportTargetAssetDataWithIds:
                            basicDictwithDataPointInformation["Existing Asset Name or Id for Mapping"] = dataPointMapping.targetAsset.neutralAssetId
                        basicDictwithDataPointInformation["Derived Asset Name Or Id"] = dataPointMapping.targetAsset.name
                        basicDictwithDataPointInformation["Derived Asset Type Id"] = dataPointMapping.targetAsset.typeId
                        basicDictwithDataPointInformation["Parent Name or Id for derived Asset"] = dataPointMapping.targetAsset.neutralParentId

                        basicDictwithDataPointInformation["Aspect Id"] = dataPointMapping.aspectId
                        basicDictwithDataPointInformation["Aspect Name"] = dataPointMapping.aspectId
                        basicDictwithDataPointInformation["Aspect Variable Name"] = dataPointMapping.variableName

                        workingDictList.append(basicDictwithDataPointInformation)
                else: # in case there are no mappings
                    workingDictList.append(workingDict)

            return workingDictList

class LibDataSource(DataSource):
    #{'name': 'DataSource1', 'customData': None, 'description': '', 
    def __init__(self, name, agentReference, description = '', customData= None):
        protocol = 'LIB'
        super().__init__(name, agentReference, protocol, description )
        self.customData = customData
        self.configurationId =  str(int(time.time()))

class HardwareSource(DataSource):
    def __init__(self,id, name, agentReference, protocol,readCycleInSeconds, description):
        super().__init__(name, agentReference, protocol, description )
        self.readCycleInSeconds = readCycleInSeconds
        self.id = id ## This is an autogenerated identifier from Mindsphere. When creating a new datapoint, this will be passed with an empty string

class OPCUAdataSource(HardwareSource):
    """ "protocolData": {
                    "opcUaServerName": "MyServerName",
                    "opcUaServerAddress": "opc.tcp://192.168.10.23:4840",
                    "opcUaServerIPAddress": "",
                    "opcUaCertificateMetadata": "",
                    "opcUaCertificate": {
                        "id": "68d82bc1-ac1b-4fe1-8a78-7c216fdeaec0",
                        "url": "https://southgate.eu1.mindsphere.io/api/deviceconfigfiles/v3/files/c7bc00a3-5e22-4352-80bd-3c0ec3e4d842/revisions/9552a7c83156a58545295915923c2d2dd0c77d80/content",
                        "name": "OpcPlc_short.der"
                    },
                    "opcUaAuthenticationType": "NONE",
                    "opcUaSecurityMode": "CERTIFICATE",
                    "opcUaUsername": "",
                    "opcUaPassword": "",
                    "enableEvents": false
                },"""

    def __init__(self, name,agentReference, protocol,id = None, readCycleInSeconds = 10, opcUaServerName = 'MyServerName#TodoChangeThis' ,opcUaServerAddress = None ,opcUaServerIPAddress ='', description =''):
        super().__init__(id, name, agentReference, protocol,readCycleInSeconds, description )
        if not opcUaServerAddress:
            print(f"No OPCUA server address has been specified for OPCUA datasource: '{name}'")
        self.opcUaServerName = opcUaServerName
        self.opcUaServerAddress = opcUaServerAddress
        self.opcUaServerIPAddress = opcUaServerIPAddress
        self.opcUaCertificateMetadata = ""
        self.opcUaCertificate = {}
        self.opcUaAuthenticationType = None 
        self.opcUaSecurityMode = None
        self.opcUaUsername =""
        self.opcUaPassword = ""
                    
    

class S7dataSource(HardwareSource):
    """   "protocolData": {
    "ipAddress": "192.168.0.1",
    "manualRackAndSlot": "Automatic",
    "rackNumber": "0",
    "slotNumber": "2" """
    def __init__(self, name, agentReference, protocol, readCycleInSeconds = 10,  ipAddress = None,id = None, manualRackAndSlot = "Automatic", rackNumber = None, slotNumber = None,description =''):
        super().__init__(id, name, agentReference, protocol,readCycleInSeconds, description)
        if not ipAddress:
            print(f"No IP-address has been specified for S7-datasource: '{name}'")
        self.ipAddress = ipAddress
        self.manualRackAndSlot = manualRackAndSlot
        self.rackNumber = rackNumber
        self.slotNumber = slotNumber
        


class DataPoint():
    def __init__(self, name, dataPointId, unit, dataType, variable, description = ''):

        self.name = name
        self.unit = unit
        self.dataType = dataType
        self.description = description
        self.dataPointId = dataPointId # Die Id kann man selber vergeben und braucht sie später in den DataMappings wieder
        self.variable = variable
        self.dataPointMappings = []
        
        self.mappingMode = None #Used in the Agent Importer context
        self.targetAssetCreationNecessary = None
        self.mappedTargetAssetDictionary = {}

    def addDatapointMapping(self, dataPointMapping):
        self.dataPointMappings.append(dataPointMapping)

class LibDataPoint(DataPoint):
    #[{'id': '1562236719938', 'name': '100000000', 'description': '', 'unit': 'C', 'type': 'DOUBLE', 'customData': None}
    def __init__(self, name, dataPointId, unit, dataType, variable, description = '', customData= None):
        super().__init__(name, dataPointId, unit, dataType, variable, description = '')
        self.customData = customData


class HardwareDataPoint(DataPoint):
    def __init__(self,dataPointId, name, dataType, variable, unit, address,hysteresis,onDataChanged, description ="", acquisitionType = None):
        super().__init__(name, dataPointId, unit, dataType,variable, description = '')
        """S7 and OPCUA Datapoint:

                        "id": "994df57a-9c6d-49e7-bfda-7fad6449e76c", # This is an autogenerated identifier from Mindsphere. When creating a new datapoint, this will be completely left out
                        "dataPointId": "ccaed787f4de4",
                        "name": "Spannung",
                        "description": "",
                        "unit": "V",
                        "dataType": "DOUBLE",
                        "dataPointData": {
                            "address": "DB2.DBD8",
                            "hysteresis": "0",
                            "onDataChanged": false """

        self.id = None # This is an autogenerated identifier from Mindsphere. When creating a new datapoint, this will be left away
        self.address = address

        if hysteresis in (False,None,""):
                hysteresis = 0
        if acquisitionType in (False,None,""):
                acquisitionType = "READ"
        if onDataChanged in (None,""):
                onDataChanged = False
    
        self.hysteresis = hysteresis   
        self.onDataChanged = onDataChanged
        self.acquisitionType = acquisitionType


class Agent():

    def __init__(self, id, securityProfile, asset, etag = 0):
        self.agentTypeId = asset.typeId
        self.relatedAsset = asset
        self.agentId = id
        self.agentName = asset.name
        self.securityProfile = securityProfile
        self.deviceConfiguration = None
        self.uploadCycle = 10 #this is only relevant for hardware devices and seems to be the standard value here: ofc this can be changed
        self.dataSources = []
        self.dataPointMappingsHaveBeenCollectedFromMindSphere = False
        self.etag = etag
        self.mappingMode = None #Used in the Agent Importer context


    def getDictListOutputForAgent(self, basicDict):

        # basicDict is an empty dict where the keys are column names
        # This is needed, that every column will be in the output file (even if it is an empty column)
        workingDictList = []
        workingDict = basicDict.copy()
        workingDict["Agent Name"] = self.agentName
        workingDict["Agent Parent Name or Id"] = self.relatedAsset.neutralParentId
        workingDict["Agent Type Id"] = self.agentTypeId
        if config.exportAgentDataWithIds:
            workingDict["Agent Name or Id"] = self.relatedAsset.neutralAssetId
        else:
            workingDict["Agent Name or Id"] = self.relatedAsset.name

        workingDict["mappingMode"] = config.defaultMappingModes
        
        if self.deviceConfiguration:
            workingDict["Serial Number"] = self.deviceConfiguration.serialNumber

            webInterface = self.deviceConfiguration.getNetworkInterface("WebInterface")
            if webInterface:
                workingDict["Web Interface DHCP"]                       = webInterface.DHCP
                workingDict["Web Interface Static IPv4"]                = webInterface.IPv4
                workingDict["Web Interface Static SubnetMask"]          = webInterface.subnetMask
                workingDict["Web Interface Static Gateway"]             = webInterface.gateway
                workingDict["Web Interface Static DNS"]                 = webInterface.DNS
            
            productionInterface = self.deviceConfiguration.getNetworkInterface("ProductionInterface")
            if productionInterface:
                workingDict["Production Interface DHCP"]                = productionInterface.DHCP
                workingDict["Production Interface Static IPv4"]         = productionInterface.IPv4
                workingDict["Production Interface Static SubnetMask"]   = productionInterface.subnetMask
                workingDict["Production Interface Static Gateway"]      = productionInterface.gateway
                workingDict["Production Interface Static DNS"]          = productionInterface.DNS
        
        if self.dataSources:
            for dataSource in self.dataSources:

                basicDictwithDataSourceInformation = workingDict.copy()

                basicDictwithDataSourceInformation["Data Source Name"]                    = dataSource.name
                basicDictwithDataSourceInformation["Data Source Description"]             = dataSource.description
                basicDictwithDataSourceInformation["Data Source Filename with Datapoints"]= dataSource.dataPointsFileName

                if dataSource.protocol in ("S7","OPCUA"):
                                    
                    basicDictwithDataSourceInformation["Data Source Protocol"]                = dataSource.protocol
                    basicDictwithDataSourceInformation["Data Source Read Cycle"]              = dataSource.readCycleInSeconds
                    if dataSource.protocol == "S7":
                        basicDictwithDataSourceInformation["Data Source IP/OPCUA Server Address"] = dataSource.ipAddress
                    if dataSource.protocol =="OPCUA":
                        basicDictwithDataSourceInformation["Data Source IP/OPCUA Server Address"] = dataSource.opcUaServerIPAddress
                
                if dataSource.protocol == "LIB":
                    pass # Do stuff that is only relevant for lib datasource configs 


                workingDictList.append(basicDictwithDataSourceInformation)
        else: #this is for an agent without any datasource definition
            workingDictList = [workingDict]
   

        return workingDictList
  

   

    def addDataSource(self, dataSource):
        self.dataSources.append(dataSource)


    
    def assignDeviceConfig(self, deviceConfiguration):
        if self.agentTypeId != "core.mclib": #this should always be the case
            if self.deviceConfiguration:
                print("Attention! The asset '{self.assetName}' already has a device Configuration. This will be overwritten now")
            self.deviceConfiguration = deviceConfiguration
        else: # for mclib items there is not device configuration
            print("Spotted a mclib object while assigna DeviceConfig in assignDeviceConfig(): This should not happen!...")


        

class Asset():
    
    importNotNecessaryList = []
    successfullyImported = []
    toBeImportedList = []
    failedList= []

    def __init__(self, name, assetId = None, parentName = None, parentId = None, description = None, ancestorOfTypeId = None, typeId = None, etag = 0):
        
        self.assetId = assetId
        self.name = name 
        self.relatedLines = []
        self.agentData = None
        if assetId:
            self.neutralAssetId = lookUpNeutralId(assetId)
        else:
            self.neutralAssetId = None
        self.parentName = parentName
        self.parentId = parentId
        self.parentAssetNameOrId = None # this is used, in case it is not clear, what kind of parent information is present (usually in an import process)
        self.neutralParentId = None

        self.typeId = typeId #This is just a string. You need to get the acutal id first
        self.ancestorOfTypeId = ancestorOfTypeId
        
        self.description = description
        self.internalId = None
        self.aspectDicts = {}
        self.timeseriesDicts = {}
        self.assetIndex = None # Optional Parameter used for multiple asset within the same hierarchy structure
        self.referenceToAssetTypeObject = None #This will contain the actual reference to the asset Type

        self.assetDescription = description
        self.assetTypeDescription = None
        self.deleteUnderlyingChilds = None
        self.deleteUnderlyingDatamodel = None
        self.offboardAgents = False
        self.childIds = []
        self.etag = etag

        
        self.importResponse = None
        self.idAfterSuccessfulImport = None
        
        self.directlyRelatedChilds = []
        self.completeChildrenTree = []
        self.error = SimpleError()

        self.alreadyExistingInMindSphere = None 
        self.suitableAssetTypeExistingInMindSphere = None
        self.assetImportStatus = None # NOT NECESSARY, FAILED, SUCCESSFUL

        self.aspectsToBeImported = []
        self.assetTypeToBeImported = None

        self.definedAspectsAsObjects = []

        self.requiredToBeImported = False

    def __eq__(self, other):

        if type(self) != type(other):
            return False
        if self == None:
            return True    
        if self.assetId != None and other.assetId != None:

            return self.assetId == other.assetId
        if  self.neutralAssetId == other.neutralAssetId and self.parentName == other.parentName:

            return self.name == other.name
    


    def __hash__(self):
            return hash(('assetId', self.assetId))
            # Keep in mind that a name fort an asset can exist multiple times (even on the same hierarchical level
    
    # The following function collects all aspects that are defined on the current Asset
    
    def linkToAssetType(self,assetTypeObject):
        self.referenceToAssetTypeObject = assetTypeObject

    def initializeAgent(self, securityProfile = 'SHARED_SECRET', etag = 0):
        self.agentData = Agent(self.assetId,securityProfile, self, etag)



    def loadAspectInformation(self,assetTypesFromMindSphere,aspectsFromMindSphere):
        #first get assettype information:
        if self.typeId.startswith("core."):
            # No need to export Core asset-types - those are usually the same in all tenants (with some exceptions)
            return
        assetTypeInMindSphere = next((x for x in assetTypesFromMindSphere if x["id"] == self.typeId), None)
        if not assetTypeInMindSphere:
            print("AssetType Defintion for an existing asset has not been found. This is somehow not possible: Probably you need to update your local repository (asset/asset-type/aspect lists)") 
            exit(0)
        
        self.assetTypeDescription = assetTypeInMindSphere["description"]

        simpleAspectDefinitionFromType = assetTypeInMindSphere["aspects"]

        for aspect in simpleAspectDefinitionFromType:
            currentAspectInMindSphere = next((x for x in aspectsFromMindSphere if x["id"] == aspect["Aspect"]["id"]), None)
            if not currentAspectInMindSphere:
                print("Aspect Defintion for an existing asset has not been found. This should not be possible: Probably you need to update your local repository (asset/asset-type/aspect lists)") 
                exit(-1)

            # Now load each single aspect ....

            aspectDescription = currentAspectInMindSphere["description"]
            aspectName =currentAspectInMindSphere["name"]
            aspectId = currentAspectInMindSphere["id"]

            currentAspectAsObject = Aspect(name = aspectName, id = aspectId, description = aspectDescription)

            currentAspectAsObject.aspectNameWithinAssetTypeContext =  aspect["name"]

            variableDefinitions = currentAspectInMindSphere["variables"]
            # ... and the data for each variables within that aspect
            for currentVariable in variableDefinitions:

                name = currentVariable["name"]
                dataType = currentVariable["dataType"]
                unit = currentVariable["unit"]
                currentAspectAsObject.addVariable(name, dataType, unit)

            self.definedAspectsAsObjects.append(currentAspectAsObject)


    def outputAsDictionaryList(self, basicDict, fullMode = False):
        # basicDict is an empty dict where the keys are column names
        # This is needed, that every column will be in the output file (even if it is an empty column)
        workingDictList = []
        workingDict = basicDict.copy()
        workingDict["Assetname"] = self.name
        workingDict["Neutral AssetId or AssetIndex"] = self.neutralAssetId
        workingDict["ParentName or ParentId"] = self.parentName
        workingDict["Neutral ParentId or ParentIndex"] = self.neutralParentId
        
        workingDict["Ancestor of AssetType"] = self.ancestorOfTypeId
        workingDict["Asset Description"] = self.description
        workingDict["Assettype"] = self.typeId
        
        
        if fullMode:
            aspects = self.referenceToAssetTypeObject.getAspects()
            if aspects:
                workingDict["AssetType Description"] = self.referenceToAssetTypeObject.description
                for aspect in aspects:
                    for variable in aspect.getVariables():
                        # Now collect and write each variable definition in the output list
                        basicDictwithAspectInformation = workingDict.copy()
                        basicDictwithAspectInformation["Aspectname"] = aspect.name
                        basicDictwithAspectInformation["Variablename"] = variable.name
                        basicDictwithAspectInformation["Datatype"] = variable.dataType
                        basicDictwithAspectInformation["Unit"] = variable.unit
                        basicDictwithAspectInformation["Aspectname Within AssetType Context"] = aspect.aspectNameWithinAssetTypeContext
                        
                        workingDictList.append(basicDictwithAspectInformation)
                        
            else: #this is usualy the case for agent assets
                workingDictList = [workingDict]
        else:
            workingDictList = [workingDict]

        return workingDictList
            

    def addAspectsToBeImported(self,aspect):
        self.aspectsToBeImported.append(aspect)
   
    def setAssetImportSucceeded(self,returnDict):
        self.assetImportStatus = ImportStatus.SUCCESSFUL
        self.assetImportStatusResponse = returnDict

    def setAssetImportFailed(self,returnDict):
        self.assetImportStatus = ImportStatus.FAILED
        self.assetImportStatusResponse = returnDict

    def setAssetDeletionFailed(self,returnDict):
        self.assetDeletionStatus = ImportStatus.FAILED
        self.assetDeletionStatusResponse = returnDict

    def setAssetDeletionSucceeded(self,returnDict):
        self.assetDeletionStatus = ImportStatus.SUCCESSFUL
        self.assetDeletionStatusResponse = returnDict

    def simpleOutput(self):
        print("{}@{}".format(self.name, self.assetId))


    def outputForDelete(self):   
        print("-" * 20)
        print("Assetname: {}".format(self.name))
        print("AssetId: {}".format(self.assetId))
        print("AssetType: {}".format(self.typeId)) 
        print("ParentId: {}".format(self.parentId)) 
        print("Asset Description: {}".format(self.description)) 
        print("Directly related Children: {}".format(str(self.directlyRelatedChilds)))
        if self.deleteUnderlyingChilds != None and self.deleteUnderlyingDatamodel != None:
            print("Delete Underlying Data Model: {}; Delete Underlying Childs: {}".format(self.deleteUnderlyingDatamodel, self.deleteUnderlyingChilds))
        else:
            print("No information about underyling child deletion set ...")
        if self.error.errorstatus:
            if self.error.hardError:
                print("HARD ERROR: {}, Message: {}".format(self.error.errorstatus, str(self.error.errortext)))
            else:
                print("Soft error: {}, Message: {}".format(self.error.errorstatus, str(self.error.errortext)))


    def output(self):   
        print("-" * 20)
        print("Assetname: {}".format(self.name))
        print("AssetId: {}".format(self.assetId))

        print("RelatedLines in Input File: {}".format(str(self.relatedLines)))
        print("Asset Description: {}".format(self.description)) 
        print("parentAssetNameOrId: {}".format(self.parentAssetNameOrId)) 
        print("ParentId: {}".format(self.parentId)) 
        print("Directly related Children: {}".format(str(self.directlyRelatedChilds)))
        print("AssetIndex: {}".format(self.assetIndex))
        print("AssetType: {}".format(self.typeId)) 
        print("AssetTypeDescription: {}".format(self.assetTypeDescription)) 
        [print("TimeseriesData at {}: {}".format(x, str(self.timeseriesDicts[x]))) for x in self.timeseriesDicts]
        [print("AspectName '{}': {}".format(x,str(self.aspectDicts[x]))) for x in self.aspectDicts]
        if self.deleteUnderlyingChilds != None and self.deleteUnderlyingDatamodel != None:
            print("Delete Underlying Data Model: {}; Delete Underlying Childs: {}".format(self.deleteUnderlyingDatamodel, self.deleteUnderlyingChilds))
        else:
            print("No information about underyling child deletion set ...")
        if self.error.errorstatus:
            if self.error.hardError:
                print("HARD ERROR: {}, Message: {}".format(self.error.errorstatus, str(self.error.errortext)))
            else:
                print("Soft error: {}, Message: {}".format(self.error.errorstatus, str(self.error.errortext)))

        print("Asset already in MindSphere: {}".format(self.alreadyExistingInMindSphere))
        print("AssetType existing in MindSphere: {}".format(self.suitableAssetTypeExistingInMindSphere))
