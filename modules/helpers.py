from pprint import pprint

from enum import IntEnum

import sys
import datetime
import traceback
import csv
import re
import modules.readConfig as config


#######################################
######### CONFIG ############
#######################################

allowedCharacters = "^[A-Za-z0-9_()\s]+$"
standardMindSphereAllowedCharactersRegEx = re.compile(allowedCharacters)


#######################################
######### HELPER FUNCTIONS ############
#######################################

thisModule = sys.modules[__name__]
requiredParamters= "logging, tenantname"
config.setSimpleConfigParametersToModule(thisModule, requiredParamters)

########
agentTypeMappingHelpers = {
    "lib" :"core.mclib",
    "mclib" :"core.mclib",
    "nano" :"core.mcnano",
    "mcnano" :"core.mcnano",
    "iot2040" :"core.mciot2040",
    "mciot2040" :"core.mciot2040"
}


        
#######################################


def writeListOfSimilarDictToCsv(listWithDicts,filename, replaceList = None):

    if listWithDicts:
        csvColumns = listWithDicts[0].keys()

        with open(filename, 'w+',newline='\n') as csvFile:
            writer = csv.DictWriter(csvFile, fieldnames = csvColumns, delimiter =";")
            writer.writeheader()
            if replaceList:
                for dictionary in listWithDicts:
                    for key in dictionary:
                        if dictionary[key]:
                            for entry in replaceList:
                                dictionary[key] = str(dictionary[key]).replace(entry["oldString"], entry["newString"])
            writer.writerows(listWithDicts)
    else:
        print(f"Nothing to write for file '{filename}' ...")

#######################################

def writeDictToCsv(dictionary, filename):

    csvColumns = dictionary.keys()

    try:
        with open(filename, 'w+') as csvFile:
            writer = csv.DictWriter(csvFile, fieldnames = csvColumns,delimiter =";")
            writer.writeheader()

            for data in dictionary:
                writer.writerow(data)

    except IOError:
        print("I/O error") 


#######################################

def readCsvIntoList(filename,separator =";"):
     # Returns a list with the various lines of the csv file as something)
    allrows =[]
    with open(filename, 'r') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=separator, quotechar='|')
        for row in spamreader:
            #print(', '.join(row))
            allrows.append(row)
    return allrows


#######################################

def readCsvIntoDict(filename,separator = ";"):
    # Returns a list of dict (one dict for each row)
    with open(filename) as csvfile: 
        allrows = []
        reader = csv.DictReader(csvfile,delimiter=separator) # quotechar='|'
        for row in reader:
            allrows.append(row)
    return allrows

#######################################


def evaluateDataWithMappingDict(currentObject, mappingDict, inputDictionary, inputLineIndex):

    currentObject.inputLineIndex = None
    for attributeName, columnDefinitions in mappingDict.items():
        currentAttributeValue = None
        allowedRegex = None
        entryMandatory = False

        if isinstance(columnDefinitions, dict):
            columnheader = columnDefinitions["matchingInputHeaders"]
            allowedRegex = columnDefinitions.get("allowedRegex")
            entryMandatory = columnDefinitions.get("entryMandatory")

        else:
             columnheader = columnDefinitions 
        
        if isinstance(columnheader, tuple): #Resolve multiple column definitions
            for entry in columnheader:
                if entry in inputDictionary:
                    columnheader = entry
                    break
            
        if  columnheader in inputDictionary:
            valueFromRow = inputDictionary[columnheader].strip()
            if valueFromRow != "":
                valueFromRow = valueFromRow.replace("[$$$TENANTNAME$$$]",tenantname)
                currentAttributeValue = valueFromRow
                if allowedRegex:
                    result = allowedRegex.match(currentAttributeValue)
                    if not result:
                        raise ValueError(f"Value '{currentAttributeValue}' in column '{columnheader}' in input line {inputLineIndex} failed to match allowed charachters ({allowedCharacters}")
            elif entryMandatory:
                raise ValueError(f"Empty value for mandatory column '{columnheader}' in input line {inputLineIndex}")
        elif entryMandatory:
            raise ValueError(f"Mandatory column '{columnheader}' missing in input data")
        
        setattr(currentObject, attributeName.strip(), currentAttributeValue)

    if inputLineIndex != None:
        lineOffset = 2 #Typical CSV Offset # TODO: In Config auslagern
        currentObject.inputLineIndex = inputLineIndex + lineOffset

    else:  
        currentObject.inputLineIndex = None

#######################################

def deriveIdFromNameOrId(nameOrId):
    if not '.' in nameOrId:
        return tenantname + "." + nameOrId
    else:
        return nameOrId

#######################################

def deriveNameFromNameOrId(nameOrId):
    if '.' in nameOrId:
        return nameOrId.split(".",1)[1]
    else:
        return nameOrId

#######################################
class ContinueHelper(Exception):
    #Helper class to continue the outer loop from within an inner loop (relevant for nested loops)
    pass

#######################################

class SimpleError():
    def __init__(self):
        self.errorstatus = False
        self.errortext = []
        self.hardError = False 

    def addError(self,message):
        self.errorstatus = True
        self.errortext.append(datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")+": " + message)

    def reset(self):
        self.errorstatus = False
        self.errortext = []
        self.hardError = False

    def addHardError(self,message):
        self.errorstatus = True
        self.addError(message)
        self.hardError = True
        print("HardError added: {}".format(message))

#######################################

class ToolMode(IntEnum):
    Importer = 1
    DeletionEngine = 2
    Exporter = 3
    WrongMode = 0

#######################################

class SingleAssetImporterInputDataset():

    # This mappingDict defines all class attributes for an instance of a row object (i. e. the mapping between the inputDatasetDictionary and the classAttributes for a row)
    # ... whereas the key of the mappingDict defines the class attribute and the value defines the corresponding key in the passed inputDictionary (which is e.g the name of the header in a csv file
    # multiple headers pointing to one class attribute are supported (to support potential different versions of the csv-input-files)
    
    mappingDict = {

        "name": {"matchingInputHeaders": "Assetname", "allowedRegex": standardMindSphereAllowedCharactersRegEx, "entryMandatory":True},
        "neutralAssetId": "Neutral AssetId or AssetIndex",
        "parentAssetNameOrId": ("ParentName or ParentId", "Parentname or ParentId","Parentname", "ParentId", "ParentName"),
        "neutralParentId": "Neutral ParentId or ParentIndex",
        "assetDescription": "Asset Description",
        
        "typeId": "Assettype",       
        "parentAssetTypeId ": ("Ancestor of AssetType","AssetTypeIsDerivationOf", "ParentTypeID","ParentAssetTypeId","ParentAssetTypeID"),
        "assetTypeDescription": "AssetType Description",

        "aspectName": "Aspectname",
        "aspectDescription": "Aspect Description",
        "variableName": "Variablename",
        #"timestamp": "Timestamp",
        #"value": "Value",
        "datatype": "Datatype",
        "unit": "Unit",
        "aspectNameWithinAssetTypeContext": "Aspectname Within AssetType Context",

        "flowMin":{"matchingInputHeaders": "Simulation Flow Min", "excludeFromExport":True},
        "flowMax":{"matchingInputHeaders": "Simulation Flow Max", "excludeFromExport":True},
        "flowLikelihood":{"matchingInputHeaders": "Simulation Flow Likelihood", "excludeFromExport":True},
        }

    def __init__(self, inputDictionary,  inputLineIndex = None):

        # This class defines itself during execution based on the parameters in the given dictionary "mappingDict".
        # Result is an instance of a row-object ()

        evaluateDataWithMappingDict(self, self.mappingDict, inputDictionary, inputLineIndex)

    def out(self):
        for attr in dir(self):
            print("obj.%s = %r" % (attr, getattr(self, attr)))


#######################################

class SingleDeletionInputDataset():


    mappingDict = {

        "deleteUnderlyingDatamodel": "Delete underlying datamodel(c -> childs only,d -> whole datamodel)",
        "offboardAgents":"Offboard Agents (y)",
        "assetNameOrId": ("AssetName or AssetId", "Assetname"), 
        "parentAssetNameOrId": "ParentName or ParentId",
        "typeId": "AssetTypeName (optional: if you want to delete standalone assetTypes, leave other columns empty then)",
        "aspectName": "AspectName (optional: if you want to delete standalone assetTypes, leave other columns empty then)",       
                }
    def __init__(self, inputDictionary,  inputLineIndex = None):
        evaluateDataWithMappingDict(self, self.mappingDict, inputDictionary, inputLineIndex)

    def out(self):
        for attr in dir(self):
            print("obj.%s = %r" % (attr, getattr(self, attr)))

#######################################

class SingleAgentInputDataset():


    mappingDict = {


        "agentNameOrId"                              :"Agent Name or Id",
        "agentTypeId"                                :"Agent Type Id",
        "agentParentNameOrId"                        :"Agent Parent Name or Id",
        
        # If agent is already existing
        "deleteMappings"                             :"Delete previously existing Mappings",
        "deleteDatasources"                          :"Delete previously existing Datasources",

        "mappingMode"                                :("Mapping Mode","mappingMode"),

        "derivedAssetNameOrId"                       :"Derived Asset Name Or Id",
        "derivedAssetTypeId"                         :"Derived Asset Type Id",
        "derivedAssetParentNameOrId"                 :"Parent Name or Id for derived Asset",

        "existingAssetNameOrIdForLinking"            :"Existing Asset Name or Id for Mapping",

        "dataSourceName"                             :"Data Source Name",
        "dataSourceProtocol"                         :"Data Source Protocol",
        "dataSourceDescription"                      :"Data Source Description",
        "dataSourceReadCycle"                        :"Data Source Read Cycle",
        "dataSourceIpOpcuaServerAddress"             :"Data Source IP/OPCUA Server Address",
        "dataSourceDataPointsFilename"               :"Data Source Filename with Datapoints",

        "aspectId"                                   :"Aspect Id", 
        "aspectName"                                 :"Aspect Name Within AssetType Context",

        "serialNumber"                               :"Serial Number",
        "webInterfaceDHCP"                           :"Web Interface DHCP",
        "webInterfaceStaticIpv4"                     :"Web Interface Static IPv4",
        "webInterfaceStaticSubnetmask"               :"Web Interface Static SubnetMask",
        "webInterfaceStaticGateway"                  :"Web Interface Static Gateway",
        "webInterfaceStaticDns"                      :"Web Interface Static DNS",
        "productionInterfaceDHCP"                    :"Production Interface DHCP",
        "productionInterfaceStaticIpv4"              :"Production Interface Static IPv4",
        "productionInterfaceStaticSubnetmask"        :"Production Interface Static SubnetMask",
        "productionInterfaceStaticGateway"           :"Production Interface Static Gateway",
        "productionInterfaceStaticDns"               :"Production Interface Static DNS"
                }
    def __init__(self, inputDictionary,  inputLineIndex = None):
        evaluateDataWithMappingDict(self, self.mappingDict, inputDictionary, inputLineIndex)

    def out(self):
        for attr in dir(self):
            print("obj.%s = %r" % (attr, getattr(self, attr)))

#######################################

class SingleDatapointInputDataset():

    mappingDict = {


        "displayName"                :"Datapoint and Variable Name",
        "dataPointId"                :"DataPoint Id",               
        "description"                :"Description",               
        "unit"                       :"Unit",       
        "datatype"                   :"DataType",           
        "address"                    :"Address",           
        "onDataChanged"              :"On Data Changed",               
        "hysteresis"                 :"Hysteresis",               
        "s7AcquistionType"           :"Acquistion Type (S7 only)",                   
        #The next parameters (if existing) would overwrite the global parementers (defined for the datasource itself)
        "derivedAssetNameOrId "      :"Derived Asset Name Or Id",                       
        "derivedAssetTypeId"         :"Derived Asset Type Id",                       
        "derivedAssetParentNameOrId": "Parent Name or Id for derived Asset",
        "existingAssetNmeOrIdMapping":"Existing Asset Name or Id for Mapping",                           
        "mappingMode"                :"Mapping Mode",               
        "aspectId"                   :"Aspect Id",           
        "aspectName"                 :"Aspect Name",
        "deviatingVariableNameAspect":"Aspect Variable Name"
        }
        
    def __init__(self, inputDictionary,  inputLineIndex = None):
        evaluateDataWithMappingDict(self, self.mappingDict, inputDictionary, inputLineIndex)

    def out(self):
        for attr in dir(self):
            print("obj.%s = %r" % (attr, getattr(self, attr)))

#######################################




