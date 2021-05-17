
import configparser
import os
import sys
import glob
import pprint
import re

from collections import OrderedDict

#######################################
############## ABSTRACT ###############
#######################################

#This module wraps given config parameters into easy accessible class/modul attributes
#If a required value is not existing, it will ask the user for manual input


#######################################
########### MODULE CONFIG #############
#######################################

# Which files are treated as config file?
configFiles = glob.glob("*.ini")

# Which configuration sections shouldn't be displayed to the user?
secretSections = ("appCredentials", "serviceCredentials")

#Provide the parameters that should be asked in manual mode. Add a list with allowed values
parametersForManualMode = OrderedDict([("toolMode",["import","delete","export","agents", "tenantmanager"]),
                            ("tenantname",None),
                            ("authenticationMode",["manual","appCredentials","browserSession"])
                           
                           ])
                           


                        
#######################################
######### HELPER FUNCTIONS ############
#######################################



def yesOrNo(question):
    reply = str(input(question+' (y/n): ')).lower().strip()
    if reply in ('y','yes'):
        return True
    elif reply in ('n','no'):
        return False
    else:
        return yesOrNo("Please Enter (y/n)\n"+question)


#######################################

def convertDataType(value):
    if value.lower() in ("1","true","yes","y"):
        return True
    if value.lower() in ("0","false","no","n",''): 
        return False
        
    if "." in value:
        try:
            return float(value)
        except ValueError: 
            return value
    else:
        try:
            return int(value)
        except ValueError: 
            return value

#######################################

def processConfigValue(value):
    processedValue = convertDataType(value)
    if isinstance(processedValue, str):
        if ("file" in option.lower() or "folder" in option.lower()) and ("/" in processedValue or "\\" in processedValue):
            processedValue = os.path.join(*(processedValue.split("/")))
        if ";" in processedValue: 
            processedValue = processedValue.split(";")
            processedValue = [x.strip() for x in processedValue if x]
    return processedValue 

#######################################

def manageMissingData(requiredValue):
    print("ATTENTION: A requested parameter '{}' is not set in the config!".format(requiredValue))
    from inspect import getframeinfo, stack

    #Also provide information from which module this request came from.
    caller = getframeinfo(stack()[2][0])
    print("Requested came from module '%s' in line '%d'" % (caller.filename, caller.lineno))
    caller = getframeinfo(stack()[3][0])
    print("Requested came from module '%s' in line '%d'" % (caller.filename, caller.lineno))
    print("You can specify the missing value in the commandline now, or you can stop the program (write 'qq') and update your config afterwards")
    newValue = input("Provide a value for '{}'\n".format(requiredValue))
    if newValue.strip().lower() == "qq":
        exit(0)
    processedValue = processConfigValue(newValue)
    return processedValue

#######################################

def overwriteParametersManually():
    
    if not(yesOrNo(f"Do you want to overwrite some parameters manually?")):
        return
    else:
        for option in parametersForManualMode:
            print("Please provide a value for '{}'".format(option))
            print ("'{}' is currently set to: \n\t---> {}\n ".format(option,configDict[option]["value"]))
            newValue = None
            if parametersForManualMode[option]:            
                print ("Allowed Values for '{}' are:".format(option))
                for index,element in enumerate(parametersForManualMode[option]):
                    print("({}) - {}".format(index +1,element))
                
                updateMissing = True
                
                while(updateMissing):
                    providedInput = input("Please speficy a new value for '{}': (Skip for no change)\n".format(option))
                    
                    try:
                        providedInput = int(providedInput)
                        if providedInput <= len(parametersForManualMode[option]):
                            newValue = parametersForManualMode[option][providedInput-1]
                            updateMissing = False
                        else:
                            print("Your input '{}' does not match the allowed values".format(providedInput))
                    except:                    
                        if providedInput in parametersForManualMode[option]:
                            newValue = providedInput
                            updateMissing = False
                        
                        elif providedInput:
                            print("Your input '{}' does not match the allowed values".format(providedInput))
                        
                        else:
                            updateMissing = False
                    
            else:
                providedInput = input("Your input: (Skip for no change)\n")
                if providedInput:
                    newValue = providedInput
            if newValue:
                configDict[option]["value"] = newValue
                
#######################################

def setSimpleConfigParametersToModule(externalModuleName, requestedParametersAsString):
    relevantParameters = map(str.strip, requestedParametersAsString.split(",")) 
    for parameter in relevantParameters:
        setattr(externalModuleName,parameter,__getattr__(parameter))


#######################################
############ MAIN BLOCK ###############
#######################################

config = configparser.ConfigParser()
config.optionxform = str 

templateRegEx =re.compile(r"{{(.*?)}}")
pp = pprint.PrettyPrinter(indent=4)


if len(configFiles)>1:
    for number, filename in enumerate(configFiles):
        print("({}.) - {}".format(number+1,filename))
    configFileNumber = input("Please choose a config file through specifying its number:\n")

    try:
        configFile = configFiles[int(configFileNumber)-1]
    except:
        print("Failed to load the specified config-number ({}), reverting back to default config".format(configFileNumber))
        configFile = 'config.ini'
        
    print("\nConfig-File to be loaded: "+ configFile)

    if not(yesOrNo(f"Continue with this config: '{configFile}'?")):
        exit(0)

else:
    configFile = configFiles[0]

config.read(configFile)

configDict = {}

#Flatten Config - Make sure that there are no duplicates
for section in config.sections():
        for option in config.options(section):
            if option in configDict:
                print("Warning: Parametername {} has been defined twice in the config. Aborting now....".format(option))
                exit(0) 
            currentValue = config.get(section, option)
            processedValue = processConfigValue(currentValue)           

            configDict[option]={"value":processedValue, "section":section}

# Ask for manual input?
overwriteParametersManually()



#pp.pprint(configDict)


# Replace template literals in Config parameters
# This will fail hard, if a template does not resolve. But that is ok.
for option in configDict:
    currentValue = configDict[option]["value"]
    if isinstance(currentValue, str):
        matches = templateRegEx.findall(currentValue)
        if matches:
            for match in matches:
                replacedValue = currentValue.replace("{{"+match+"}}", configDict[match]["value"])
                
            configDict[option]["value"] = replacedValue
        

# Print out 
print("\nThe tool will run with the following configuration:\n")
for option in configDict:
    
    if configDict[option]["section"] not in secretSections:
            value = configDict[option]["value"]
            section = configDict[option]["section"]
            dataType = type(value)
            print("{:<60} --> {:>40} ".format(section +" - " + option,str(value)))
            #print("{:<60} --> {:>50} ({})".format(section +" - " + option,str(value), dataType))
print("\n")

#######################################
########### MODULE WRAPPER ############
#######################################

# This wrapper is necessary that calling modules can call module attributes in a standard way, even if those are actually not existing yet.
# In case of missing parameters, the user of the application will be asked for a direct input of them.

# In case a python version below 3.7 is used, wrapping has to be done with a seperate class...
if int(sys.version_info[1])<7:
    class Wrapper(object):
        def __init__(self, wrapped):
            self.wrapped = wrapped
        def __getattr__(self, configurationOption):
            try:
                return configDict[configurationOption]["value"]
                
            except KeyError:
                newData = manageMissingData(configurationOption)
                configDict[configurationOption]={"value":newData, "section":"manualInput"}
                return newData
                
    currentModuleName =  __name__
    sys.modules[currentModuleName] = Wrapper(sys.modules[currentModuleName])

# With a higher Python version such a change of a module's __getattr__ function is already supported by Python itself (without having to wrap it into a seperate class)
else:
    def __getattr__(configurationOption):            
        if configurationOption in configDict:

            return configDict[configurationOption]["value"]
        else:
            newData = manageMissingData(configurationOption)
            configDict[configurationOption]={"value":newData, "section":"manualInput"}
            return newData
            
