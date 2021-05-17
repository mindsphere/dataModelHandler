from pprint import pprint

import helpers
import pdb
#Custom Modules
import modules.readConfig as config
from modules.datamodelClassDefinitions import (Agent, Aspect, Asset, AssetType,
                                               DataPointMapping,
                                               DeviceConfiguration,
                                               HardwareDataPoint, ImportStatus,
                                               LibDataPoint, LibDataSource,
                                               NetworkInterface,
                                               OPCUAdataSource, S7dataSource)
from modules.helpers import (SingleAgentInputDataset,
                             SingleAssetImporterInputDataset,
                             SingleDatapointInputDataset,
                             SingleDeletionInputDataset)

assetTypeList = []
aspectList = []
assetDict = {}


def createVirtualTargetAssetsFromDict(mindsphereDataModelManager):
    mappingTable = {}
    assetList = []

    for mappingKey in assetDict:
        targetAssetDict = assetDict[mappingKey]
        
        # Evaluate current TargetAssets for Mappings:
        if "derivedAssetNameOrId" in targetAssetDict:
            targetAssetInMindSphere = mindsphereDataModelManager.getAssetsIncludingUserDecision(targetAssetDict["derivedAssetNameOrId"],allowToChooseNone=True) 
            #Todo: think about if it is allowed to chose "none" here
            
            if targetAssetInMindSphere:
                #Target Asset is already existing, there is no need to derive anything for now.
                #Todo: Check if type of target asset fits the required type and optionally expand type
                targetAssetInMindSphere[0].requiredToBeImported = False
                assetList.append(targetAssetInMindSphere[0])
                mappingTable[mappingKey] = targetAssetInMindSphere[0]
            
                
            else: # Target asset is not existing yet:
                assetTypeLowerCase = targetAssetDict["derivedAssetTypeId"].lower()
                if assetTypeLowerCase in helpers.agentTypeMappingHelpers:
                    print("AssetType '{}' not allowed for a target asset '{}'".format(targetAssetDict["derivedAssetTypeId"], targetAssetDict["assetNameOrId"]))
                    exit(-1)
                
                typeId = helpers.deriveIdFromNameOrId(targetAssetDict["derivedAssetTypeId"])
                
                if targetAssetDict["derivedAssetParentNameOrId"]:
                    parentInMindSphere = mindsphereDataModelManager.getAssetsIncludingUserDecision(targetAssetDict["derivedAssetParentNameOrId"])
                    if not parentInMindSphere: 
                        print("Parent '{}' not existing for TargetAsset '{}' not existing in MindSphere".format(targetAssetDict["agentParentNameOrId"],targetAssetDict["assetNameOrId"]))
                        exit(-1)
                parentInMindSphere = parentInMindSphere[0]     
                #Now create target Assetobject:
                currentAsset = Asset(targetAssetDict["derivedAssetNameOrId"], parentId = parentInMindSphere.assetId, typeId=typeId)
                currentAsset.requiredToBeImported = True
                #Check if the related asset Type has alread been found:
                
                newAssetType = next((x for x in assetTypeList if x.id == targetAssetDict["derivedAssetTypeId"]),None)
                if not newAssetType:                                      
                    newAssetType = AssetType(typeId, helpers.deriveNameFromNameOrId(typeId), config.defaultAssetTypeDescription, config.defaultParentAssetTypeId)
                    assetTypeList.append(newAssetType)

                currentAsset.referenceToAssetTypeObject = newAssetType
                for aspectId in targetAssetDict["aspects"]:
                    aspectDefinition = targetAssetDict["aspects"][aspectId][0]
                    aspectName = targetAssetDict["aspects"][aspectId][1]
                    newAspect = next((x for x in aspectList if x.id == aspectId),None)
                    if not newAspect:  #Aspect did not show up before...                                    
                        newAspect = Aspect(aspectId,helpers.deriveIdFromNameOrId(aspectId), config.defaultAspectDescription, aspectNameWithinAssetTypeContext = aspectName)
                        newAspect.aspectNameWithinAssetTypeContext = aspectName
                        aspectList.append(newAspect)
                    for variableName in aspectDefinition:

                        newVariable = next((x for x in newAspect.variables if x.name == variableName),None)

                        if not newVariable:
                            newAspect.addVariable(variableName,aspectDefinition[variableName]["dataType"],aspectDefinition[variableName]["unit"])

                    aspectIsAlreadyExistingOnAssetType = next((x for x in newAssetType.getAspects() if x.id == newAspect.id and x.aspectNameWithinAssetTypeContext == newAspect.aspectNameWithinAssetTypeContext),None)
                    if not aspectIsAlreadyExistingOnAssetType:
                        newAssetType.addAspect(newAspect)
                        newAssetType.addAspectThatNeedsImporting(newAspect)

                assetList.append(currentAsset)
                mappingTable[mappingKey] = currentAsset
        else:

            print(f"No Target asset name found for this definition {targetAssetDict}")

    return assetList, mappingTable


def convertRowsToClasses(csvContentAsDict, mode = None):

    allInputRows = []
    for idx,entry in enumerate(csvContentAsDict):
        if not mode:
            if config.toolMode == "import":
                currentInputDataset = SingleAssetImporterInputDataset(entry,idx)

            if config.toolMode == "delete":
                currentInputDataset = SingleDeletionInputDataset(entry,idx)

            if config.toolMode == "agents":
                currentInputDataset = SingleAgentInputDataset(entry,idx)
        else:
            if mode == "dataPointMappings":
                currentInputDataset = SingleDatapointInputDataset(entry,idx)
        # DEBUG pprint(vars(currentInputDataset))
        allInputRows.append(currentInputDataset)

    return allInputRows


def verifyMappingModes(mappingMode):
    for element in mappingMode.split(","):
        if element.strip() not in ("CreateAgent","DeriveAsset","DeriveType","Map"):
            print(f"Wrong Mapping mode specified: '{mappingMode}'")
            exit(-1)
        else:
            return mappingMode

def populateDefaults():
    #TODO: Populate defaults already after CSV loading has been finished
    pass

def extractAssetDefinitions(currentRowAsClass):

    assetList = []
    errorAssetList = []

    for row in currentRowAsClass:

        newAsset = False
        # remove all lines without assetname 
        if not row.name:  
            currentAsset = Asset("<MISSING NAME>")
            currentAsset.relatedLines.append(row.inputLineIndex)
            currentAsset.error.addError("Assetname missing in line {}".format(row.inputLineIndex))
            errorAssetList.append(currentAsset)
            continue 

        # remove all line, where assetname = parentname -> This will not be evaluated since it does not make sense
        if row.name == row.parentAssetNameOrId:
            currentAsset = Asset(row.name.strip())
            currentAsset.relatedLines.append(row.inputLineIndex)
            currentAsset.error.addError("Parentname equals Assetname in line {}".format(row.inputLineIndex))
            errorAssetList.append(currentAsset)
            continue
                
        # Check if current Asset is already in list (via name, parent and index), and if yes, fetch reference to it 
        currentAsset = next((x for x in assetList if x.name == row.name \
                                                    and x.parentAssetNameOrId == row.parentAssetNameOrId.strip() \
                                                    and x.neutralAssetId == row.neutralAssetId), None)

        if not currentAsset: #If it does not exist in the currently generated list yet, create a new emtpy asset, with basic information
            newAsset = True
            currentAsset = Asset(row.name) # and assign name
            if row.parentAssetNameOrId:
                row.parentAssetNameOrId = row.parentAssetNameOrId.strip()
            currentAsset.parentAssetNameOrId = row.parentAssetNameOrId # and the optional  filled out parent
            currentAsset.neutralAssetId = row.neutralAssetId # and optional filled out asset index
            currentAsset.neutralParentId = row.neutralParentId
        
        currentAsset.relatedLines.append(row.inputLineIndex) #no matter if asset was created or was already existing, add the current related line to this asset


        # Set ParentAssetTypeID: There should be at least one in one of the lines related to an asset (otherwise default will be taken later)
        if row.parentAssetTypeId:
            if currentAsset.ancestorOfTypeId != row.parentAssetTypeId and currentAsset.ancestorOfTypeId:
                # Complain, if different asset descriptions are available for an asset, but dont abort - the first description found will be taken ...
                currentAsset.error.addError("Divergent definition of ParentTypeID found in line {}. Existing Defintion: {}, Divergent Definition: {}".format(row.inputLineIndex,currentAsset.ancestorOfTypeId,row.parentAssetTypeId))
            else:
                currentAsset.ancestorOfTypeId = row.parentAssetTypeId

        # Set Asset Description: There should be at least one in one of the lines related to an asset (otherwise default will be taken later)
        if row.assetDescription:
            if currentAsset.description != row.assetDescription and currentAsset.description:
                # Complain, if different asset descriptions are available for an asset, but dont abort - the first description found will be taken ...
                currentAsset.error.addError("Divergent definition of Asset Descriptions found in line {}. Existing Defintion: {}, Divergent Definition: {}".format(row.inputLineIndex,currentAsset.description,row.assetDescription))
            else:
                currentAsset.description = row.assetDescription
        
        ## Next block should be handled different 
        # Set AssetType Description: There should be at least one in one of the lines related to an asset (otherwise default will be taken later)
        """if entry["AssetType Description"] not in ("", None):
            if currentAsset.typeIdDescription != entry["AssetType Description"] and currentAsset.typeIdDescription not in ("", None):
                # Complain, if different asset Type descriptions are available for an asset, but dont abort - the first description found will be taken ...
                currentAsset.error.addError("Divergent definition of AssetType Descriptions found in line {}".format(idx+2))
            else:
                currentAsset.typeIdDescription = entry["AssetType Description"]
        """

        # Set AssetType: There should be at least one in one of the lines related to an asset (otherwise default will be taken later)
        if row.typeId:
            if currentAsset.typeId != row.typeId and currentAsset.typeId:
                # Complain, if different asset descriptions are available for an asset, but dont abort - the first description found will be taken ...
                currentAsset.error.addError("Divergent definition of AssetTypes found in line {}. Existing Defintion: {}, Divergent Definition: {}".format(row.inputLineIndex,currentAsset.typeId, row.typeId))
            else:
                currentAsset.typeId = row.typeId
        
        if row.aspectName:
        #Now check and add the aspectname
            currentAspectName = row.aspectName #AspectName auslesen (ohne leading tenantname)
        
            currentAspectNameInAssetTypeContext = row.aspectNameWithinAssetTypeContext

            aspectNameWithinAssetTypeContext = currentAspectName #The default would be, that the name in the assetType context is the same as the aspect name itself

            if currentAspectNameInAssetTypeContext != None and currentAspectNameInAssetTypeContext != currentAspectName:
                #apparently the name of the aspect within the assetType definition differs from the aspect name itself
                aspectNameWithinAssetTypeContext = currentAspectNameInAssetTypeContext

            if aspectNameWithinAssetTypeContext not in currentAsset.aspectDicts:
                currentAspectDict = {}
                currentAspectDict["InternalAspectName"] = currentAspectName #in each case: save the actual internal aspect name
                currentAspectDict["VariableDefinition"] = {}
                currentAspectDict["Aspect Description"] = ""
                
            else:
                currentAspectDict = currentAsset.aspectDicts[aspectNameWithinAssetTypeContext]

            currentVariableName = row.variableName
            if currentVariableName and currentVariableName not in currentAspectDict["VariableDefinition"]:
                currentAspectDict["VariableDefinition"][currentVariableName]= {"Datatype": row.datatype, "Unit": row.unit, "RelatedLine" : row.inputLineIndex, "SimulatedFlowMin": row.flowMin, "SimulatedFlowMax": row.flowMax,"SimulatedFlowLikelihood": row.flowLikelihood }

            currentAspectDescription = currentAspectDict["Aspect Description"]
            # Set Aspect Description: There should be at least one for each defined aspect
            if row.aspectDescription:
                if currentAspectDescription and currentAspectDescription != row.aspectDescription:
                    # Complain, if different asset descriptions are available for an asset, but dont abort - the first description found will be taken ...
                    currentAsset.error.addError("Divergent definition of AspectDescription found in line {}".format(row.inputLineIndex))
                else:
                    currentAspectDict["Aspect Description"] = row.aspectDescription

            currentAsset.aspectDicts[aspectNameWithinAssetTypeContext] = currentAspectDict
 

        if newAsset:
            assetList.append(currentAsset)

    #Now aggregate aspect Informations from all type definitions:
    allExistingTypeIds = list(set([currentAsset.typeId for currentAsset in assetList]))

    for typeId in allExistingTypeIds:
        aggregatedDict = {}
        assetsWithSameType = [currentAsset for currentAsset in assetList if typeId == currentAsset.typeId ]
        for asset in assetsWithSameType:
            aggregatedDict.update(asset.aspectDicts)
        for asset in assetsWithSameType:
            asset.aspectDicts = aggregatedDict
    return assetList, errorAssetList

def extractAspectDefinitions(currentRowAsClass):
    #TODO: For Standalone Aspect Import
    aspectList = []
    errorAspectList = []
    
    return (aspectList, errorAspectList)
    

def extractAssetTypeDefinitions(currentRowAsClass):
    # TODO For Standalone AssetType Import
    assetTypeList = []
    errorAssetTypeList = []

    return (assetTypeList, errorAssetTypeList)



def extractAgentDefinitions(agentDefinitionRowsAsClasses, mindsphereDataModelManager):
    agentList = []
    errorAgentList = []

    for row in agentDefinitionRowsAsClasses:
        
        if not row.agentNameOrId:
            currentAgentAsset = Asset("<MISSING NAME>")
            currentAgentAsset.relatedLines.append(row.inputLineIndex)
            error = "Agentname missing in line {}".format(row.inputLineIndex)
            print(error)
            currentAgentAsset.error.addError(error)
            errorAgentList.append(currentAgentAsset)
            continue
        
        #Preprocess Types to allow omitting CORE attribute
        row.agentTypeId = row.agentTypeId.lower()
        if row.agentTypeId in helpers.agentTypeMappingHelpers:
            row.agentTypeId = helpers.agentTypeMappingHelpers[row.agentTypeId]

        if row.agentTypeId!= None and row.agentTypeId not in ("core.mclib, core.mcnano,core.mciot2040"):
            currentAgentAsset = Asset(row.agentNameOrId)
            currentAgentAsset.relatedLines.append(row.inputLineIndex)
            error = "Unknown Agent-Type '{}' for agent '{}'(line {})".format(row.agentTypeId, row.agentNameOrId, row.inputLineIndex)
            print(error)
            currentAgentAsset.error.addError(error)
            errorAgentList.append(currentAgentAsset)
            continue
        
        if not row.agentParentNameOrId:
            row.agentParentNameOrId = config.defaultParentAssetName

        parentInMindSphere = mindsphereDataModelManager.getAssetsIncludingUserDecision(row.agentParentNameOrId, allowToEnterOwnId = True) #here was a non-resolved keyword: searchForParent = True
        if not parentInMindSphere: 
            currentAgentAsset = Asset(row.agentNameOrId)
            currentAgentAsset.relatedLines.append(row.inputLineIndex)
            error = "Parent '{}' not existing for Agent '{}' not existing in MindSphere (line {})".format(row.agentParentNameOrId,row.agentNameOrId, row.inputLineIndex)
            print(error)
            currentAgentAsset.error.addError(error)
            errorAgentList.append(currentAgentAsset)
            continue
        parentInMindSphere = parentInMindSphere[0]

        assetsInMindSphere = mindsphereDataModelManager.getAssetsIncludingUserDecision(row.agentNameOrId, allowToChooseNone=True )
        
        if assetsInMindSphere:
            #This means an already existing agent should be updated or changed - not implemented yet:
            print(f"Agent '{row.agentNameOrId}' is already existing in MindSphere - updating existing agents is not yet implemented correctly. You could change the agent name or delete the existing agent first")
            if not config.yesOrNo("Do you still want to continue?"):
                exit(0)
            currentAgentAsset = assetsInMindSphere[0]
            currentAgentAsset.alreadyExistingInMindSphere = True
            
                        


        else:
            #Create internal agent-asset object
            currentAgentAsset = Asset(row.agentNameOrId, parentId = parentInMindSphere.assetId, typeId = row.agentTypeId)

        # Check if the object is already in the list
        alreadyExistingAgentAsset = next((x for x in agentList if x == currentAgentAsset),None)
        if alreadyExistingAgentAsset:
            currentAgentAsset = alreadyExistingAgentAsset
        
        else: #if not, add it
            currentAgentAsset.initializeAgent()
            if not row.mappingMode:
                currentAgentAsset.agentData.mappingMode = config.defaultMappingModes
            else:
                currentAgentAsset.agentData.mappingMode = row.mappingMode

            agentList.append(currentAgentAsset)
        if not currentAgentAsset.agentData.deviceConfiguration: #The first device configuration found in the definition file will be taken over
            if currentAgentAsset.typeId != "core.mclib":
                deviceType = "NANO" if "nano" in currentAgentAsset.typeId else "IOT2040"
                currentDeviceConfiguration = DeviceConfiguration(
                    deviceType = deviceType,
                    serialNumber = row.serialNumber
                )
                currentDeviceConfiguration.addNetworkInterface(NetworkInterface(
                    name = "WebInterface",
                    DHCP=row.webInterfaceDHCP or True,
                    IPv4=row.webInterfaceStaticIpv4,
                    subnetMask=row.webInterfaceStaticSubnetmask,
                    gateway=row.webInterfaceStaticGateway,
                    DNS=row.webInterfaceStaticDns,
                    )
                )
                currentDeviceConfiguration.addNetworkInterface(NetworkInterface(
                    name = "ProductionInterface",
                    DHCP=row.productionInterfaceDHCP or True,
                    IPv4=row.productionInterfaceStaticIpv4,
                    subnetMask=row.productionInterfaceStaticSubnetmask,
                    gateway=row.productionInterfaceStaticGateway,
                    DNS=row.productionInterfaceStaticDns
                    )
                )
                currentAgentAsset.agentData.deviceConfiguration = currentDeviceConfiguration
        # Evaluate Current DataSource Definition
        # Todo add some more consistence checks
        if row.dataSourceName!= None:
            if currentAgentAsset.typeId == "core.mclib":
                currentDataSource = LibDataSource(
                name = row.dataSourceName, 
                agentReference = currentAgentAsset, 
                description = row.dataSourceDescription)

            elif row.dataSourceProtocol == "S7":
                if row.dataSourceIpOpcuaServerAddress == None:
                    print(f"Missing IP adress for datasource '{row.dataSourceName}' of '{row.agentNameOrId}' - enter it in the input data")
                    exit(-1)
                currentDataSource = S7dataSource(
                name = row.dataSourceName, 
                agentReference = currentAgentAsset, 
                protocol = row.dataSourceProtocol,
                description = row.dataSourceDescription,
                readCycleInSeconds = config.defaultDataSourceReadCycle,
                ipAddress = row.dataSourceIpOpcuaServerAddress) 
                # todo NetworkInterface Config hinzubauen
             
            elif row.dataSourceProtocol == "OPCUA":
                if row.dataSourceIpOpcuaServerAddress == None:
                    print(f"Missing IP adress for datasource '{row.dataSourceName}' of '{row.agentNameOrId}' - enter it in the input data")
                    exit(-1)
                currentDataSource = OPCUAdataSource(
                name = row.dataSourceName, 
                agentReference = currentAgentAsset, 
                protocol = row.dataSourceProtocol,
                description = row.dataSourceDescription,
                readCycleInSeconds = config.defaultDataSourceReadCycle,
                opcUaServerAddress = row.dataSourceIpOpcuaServerAddress)
            else:
                print(f"A hardware Agent  '{row.agentNameOrId}' is meant to be imported, but the input data does not provide data for property 'protocol' - enter S7 or OPCUA there")
                exit(-1)
                # todo Network Config hinzubauen
            if row.dataSourceDataPointsFilename:
                currentDataSource.dataPointsFileName = row.dataSourceDataPointsFilename
            else:
                currentDataSource.dataPointsFileName =  row.dataSourceName +"_datapoints.csv"
    

            if not row.mappingMode:
                row.mappingMode = config.defaultMappingModes

            currentDataSource.mappingMode = verifyMappingModes(row.mappingMode)

            currentDataSource.mappedTargetAssetDictionary["derivedAssetNameOrId"] = row.derivedAssetNameOrId
            currentDataSource.mappedTargetAssetDictionary["derivedAssetTypeId"] = row.derivedAssetTypeId
            currentDataSource.mappedTargetAssetDictionary["derivedAssetParentNameOrId"] = row.derivedAssetParentNameOrId
            
            if not row.aspectId and row.aspectName:
                row.aspectId = row.aspectName
            elif not row.aspectName and row.aspectId:
                row.aspectName = row.aspectId
            elif not row.aspectName and not row.aspectId:
                row.aspectId = currentDataSource.name
                row.aspectName = currentDataSource.name

            currentDataSource.mappedTargetAssetDictionary["aspectId"] = row.aspectId
            currentDataSource.mappedTargetAssetDictionary["aspectName"] = row.aspectName
            # addDatasourceDefinition to agent:
            currentAgentAsset.agentData.addDataSource(currentDataSource)

    return agentList, errorAgentList


def extractAndAttachDataPointDefinitions(agentAsset, dataSource, dataPointRowsAsClasses, mindsphereDataModelManager):
    # This functions extracts all datapoints and all related datapoint mappings from a datapoint definition file (which is related to a datasource)

    for row in dataPointRowsAsClasses:
        defaultMappingMode = dataSource.mappingMode

        defaultTargetAssetDictionary = dataSource.mappedTargetAssetDictionary

        #check if this row is already seen before as a datapoint definition
        currentDataPoint = next((x for x in dataSource.dataPoints if x.dataPointId == row.dataPointId), None)

        if not currentDataPoint:
            if row.deviatingVariableNameAspect in (None,'',"None"):

                row.deviatingVariableNameAspect = row.displayName #in case there is no variable name defined, the display name is used - and the other way round

            if row.deviatingVariableNameAspect.strip() in (None,'',"None"):
                print(f"No variable has been defined for agent '{agentAsset.name}' in datasource {dataSource.name}")
                pprint(vars(row))
                exit(-1)
                

            if dataSource.protocol == "S7" and row.address == None:
                print(f"DB-Address missing for Hardware agent '{agentAsset.name}' in datapoint definition for data source '{dataSource.name}'. This relates to the following datapoint input dataset:")
                pprint(vars(row))
                exit(-1)

            if agentAsset.typeId == "core.mclib":
                currentDatapoint = LibDataPoint(
                    name = row.displayName,
                    dataPointId = row.dataPointId,
                    unit = row.unit,
                    dataType = row.datatype.upper(),
                    variable = row.deviatingVariableNameAspect,
                    description = row.description
                )
            else:
                currentDatapoint = HardwareDataPoint(
                    name=row.displayName,
                    dataPointId=row.dataPointId,
                    unit=row.unit,
                    dataType=row.datatype.upper(),
                    variable = row.deviatingVariableNameAspect,
                    description=row.description,
                    address=row.address,
                    hysteresis=row.hysteresis,
                    onDataChanged =row.onDataChanged,
                    acquisitionType=row.s7AcquistionType
                )


            if row.mappingMode!= None:
                mappingMode = row.mappingMode
            else:
                #No individual Mapping Mode is available, so use the default from the DataSource
                mappingMode = defaultMappingMode

            mappingMode = verifyMappingModes(mappingMode)
            currentDatapoint.mappingMode = mappingMode
            dataSource.addDataPoint(currentDatapoint)

            if any(x for x in currentDatapoint.mappingMode.split(',') if x.strip() in ( "DeriveType", "DeriveAsset", "Map")):
                # This is an entry where information about mappings should be processed.
                # There fore a datapoint Mapping will be created
                if not row.aspectId and row.aspectName:
                    row.aspectId = row.aspectName
                elif not row.aspectName and row.aspectId:
                    row.aspectName = row.aspectId
                elif not row.aspectName and not row.aspectId:
                    row.aspectId = defaultTargetAssetDictionary["aspectId"]
                    row.aspectName = defaultTargetAssetDictionary["aspectName"]
                    
                individualMappedTargetAssetDictionary = {}
                individualMappedTargetAssetDictionary["derivedAssetNameOrId"] = row.derivedAssetNameOrId if row.derivedAssetNameOrId else defaultTargetAssetDictionary["derivedAssetNameOrId"]
                individualMappedTargetAssetDictionary["derivedAssetTypeId"] = row.derivedAssetTypeId if row.derivedAssetTypeId else defaultTargetAssetDictionary["derivedAssetTypeId"]
                individualMappedTargetAssetDictionary["derivedAssetParentNameOrId"] = row.derivedAssetParentNameOrId if row.derivedAssetParentNameOrId else defaultTargetAssetDictionary["derivedAssetParentNameOrId"]
                individualMappedTargetAssetDictionary["aspectId"] = row.aspectId
                individualMappedTargetAssetDictionary["aspectName"] = row.aspectName
                individualMappedTargetAssetDictionary["variableName"] = row.deviatingVariableNameAspect
                individualMappedTargetAssetDictionary["unit"] = row.unit
                individualMappedTargetAssetDictionary["dataType"] = row.datatype
                
                currentDatapoint.mappedTargetAssetDictionary = individualMappedTargetAssetDictionary

                #todo rework this
                if individualMappedTargetAssetDictionary["derivedAssetParentNameOrId"] == None:
                    individualMappedTargetAssetDictionary["derivedAssetParentNameOrId"] = "PARENT_NOT_SPECIFIED"
                if individualMappedTargetAssetDictionary["derivedAssetTypeId"] == None:
                    individualMappedTargetAssetDictionary["derivedAssetTypeId"] = "TYPE_NOT_SPECIFIED"                    
                uniqueTargetAssetIdentifier = individualMappedTargetAssetDictionary["derivedAssetNameOrId"] +"___" + individualMappedTargetAssetDictionary["derivedAssetParentNameOrId"] + "___" + individualMappedTargetAssetDictionary["derivedAssetTypeId"]
                if uniqueTargetAssetIdentifier in assetDict:
                    partlyAsset = assetDict[uniqueTargetAssetIdentifier]

                else:
                    partlyAsset = {   
                        "derivedAssetNameOrId": individualMappedTargetAssetDictionary["derivedAssetNameOrId"],
                        "derivedAssetTypeId": individualMappedTargetAssetDictionary["derivedAssetTypeId"],
                        "derivedAssetParentNameOrId":individualMappedTargetAssetDictionary["derivedAssetParentNameOrId"],
                        
                        "aspects" :{}
                    }
                    assetDict[uniqueTargetAssetIdentifier] = partlyAsset


                if individualMappedTargetAssetDictionary["aspectId"] in partlyAsset["aspects"]:
                    currentAspect = partlyAsset["aspects"][individualMappedTargetAssetDictionary["aspectId"]][0]
                    
                else:
                    currentAspect = {}
                    currentAspectName = individualMappedTargetAssetDictionary["aspectName"]
                    partlyAsset["aspects"][individualMappedTargetAssetDictionary["aspectId"]] = (currentAspect, currentAspectName)

                #print(individualMappedTargetAssetDictionary["variableName"])

                if not individualMappedTargetAssetDictionary["variableName"] in currentAspect:
                    currentAspect[individualMappedTargetAssetDictionary["variableName"]] = {"dataType":individualMappedTargetAssetDictionary["dataType"], "unit":individualMappedTargetAssetDictionary["unit"]}
                else:
                    pass

                currentDataPointMapping = DataPointMapping(agentAsset = agentAsset,targetAsset = uniqueTargetAssetIdentifier, dataPointId = row.dataPointId, aspectId = individualMappedTargetAssetDictionary["aspectId"], variableName = individualMappedTargetAssetDictionary["variableName"])
                currentDataPointMapping.mappingMode = mappingMode

                currentDatapoint.dataPointMappings.append(currentDataPointMapping)



def extractDataFromDeletionInputList(currentRowAsClass, mindsphereDataModelManager):
    initialAssetList = []  
    initialAssetTypeList = []
    initialAspectList = []
    errorAssetList = []
    errorAssetTypeList = []
    errorAspectList = []

    for row in currentRowAsClass:

        if row.assetNameOrId:
            
            if not row.parentAssetNameOrId: #Wenn es auch kein Parent gibt, wird überall gesucht
                assetsInMindSphere = mindsphereDataModelManager.getAssetsIncludingUserDecision(row.assetNameOrId, allowMultipleSelections=True )
            
            else:
                parentInMindSphere = mindsphereDataModelManager.getAssetsIncludingUserDecision(row.parentAssetNameOrId)

                if not parentInMindSphere:
                    currentAsset = Asset(row.name)
                    currentAsset.relatedLines.append(row.inputLineIndex)
                    currentAsset.error.addError("No parent-ids found for given Parent-Identifier '{}' in input line {} ".format(row.parentAssetNameOrId, row.inputLineIndex))
                    errorAssetList.append(currentAsset)
                    continue
                
                else:
                    assetsInMindSphere = mindsphereDataModelManager.getAssetsIncludingUserDecision(row.name, parentId = parentInMindSphere.assetId, allowMultipleSelections=True )

            
            if not assetsInMindSphere:
                currentAsset = Asset(row.assetNameOrId)
                currentAsset.relatedLines.append(row.inputLineIndex)
                currentAsset.error.addError("No asset found for given Asset-Identifier '{}' and given Parent-Identifier '{}' in input line {} ".format(row.assetNameOrId, row.parentAssetNameOrId, row.inputLineIndex))
                errorAssetList.append(currentAsset)
                continue


            for assetInMindSphere in assetsInMindSphere:

                if row.deleteUnderlyingDatamodel in (None,""):
                    assetInMindSphere.deleteUnderlyingChilds = False 
                    assetInMindSphere.deleteUnderlyingDatamodel = False

                if row.deleteUnderlyingDatamodel == 'd':
                    assetInMindSphere.deleteUnderlyingChilds = True 
                    assetInMindSphere.deleteUnderlyingDatamodel = True 

                if row.deleteUnderlyingDatamodel == 'c':
                    assetInMindSphere.deleteUnderlyingChilds = True 
                    assetInMindSphere.deleteUnderlyingDatamodel = False

                if row.offboardAgents == 'y':
                    assetInMindSphere.offboardAgents = True 

                assetInMindSphere.relatedLines.append(row.inputLineIndex)

                #Check if asset is already in list (via AssetID):
                assetAlreadyInList = next((x for x in initialAssetList if x.assetId == assetInMindSphere.assetId), None)
            
                if not assetAlreadyInList:
                    initialAssetList.append(assetInMindSphere)
                else:
                    currentAsset.error.addError("Asset with name '{}' and and assetId '{}' from input line {} has already been defined before. This entry will be ignored".format(currentAsset.name,currentAsset.assetId, row.inputLineIndex))
                    errorAssetList.append(assetInMindSphere)
                continue

        elif row.typeId:
            assetTypeInMindSphere = mindsphereDataModelManager.getAssetType(helpers.deriveIdFromNameOrId(row.typeId))

            if assetTypeInMindSphere:

                if row.deleteUnderlyingDatamodel == 'd':
                    assetTypeInMindSphere.deleteUnderlyingChilds = True 
                    assetTypeInMindSphere.deleteUnderlyingDatamodel = True 
                if row.deleteUnderlyingDatamodel == 'c':
                    assetTypeInMindSphere.deleteUnderlyingChilds = True 

                assetTypeInMindSphere.relatedLines.append(row.inputLineIndex)

                initialAssetTypeList.append(assetTypeInMindSphere)
                continue
        
            else:
                currentAssetType = AssetType(name = helpers.deriveNameFromNameOrId(row.typeId), id = helpers.deriveIdFromNameOrId(row.typeId))
                currentAssetType.relatedLines.append(row.inputLineIndex)
                currentAssetType.error.addError("No assetType found for given assetTypeName '{}' in input line {} ".format(row.typeId, row.inputLineIndex))
                errorAssetTypeList.append(currentAssetType)
                continue

        elif row.aspectName:
            aspectInMindSphere = mindsphereDataModelManager.getAspect(helpers.deriveIdFromNameOrId(row.aspectName))
            aspectInMindSphere 

            if aspectInMindSphere:
                
                aspectInMindSphere.relatedLines.append(row.inputLineIndex)

                initialAspectList.append(aspectInMindSphere)
                continue
        
            else:
                currentAspect = Aspect(name = helpers.deriveNameFromNameOrId(row.aspectName), id = helpers.deriveIdFromNameOrId(row.aspectName))
                currentAspect.relatedLines.append(row.inputLineIndex)
                currentAspect.error.addError("No Aspect found for given AspectName '{}' in input line {} ".format(row.aspectName, row.inputLineIndex))
                errorAspectList.append(currentAspect)
                continue            

    return  initialAssetList,    initialAssetTypeList,    initialAspectList,    errorAssetList ,    errorAssetTypeList ,    errorAspectList
