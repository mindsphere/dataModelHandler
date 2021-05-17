import base64
import traceback
from datetime import date, datetime
import os
import re
import sys
import traceback
import json
import requests
import uuid
import pprint
import modules.readConfig as config
from types import SimpleNamespace


### Minimum Python Version: 3.7
DEBUG = False
#######################################
########### MODULE CONFIG #############
#######################################
# The following block loads parameters from the config an provides them in an easy to use way in this modul:
# Instead of config.<parametername> you can just use <parametername> afterwards
thisModule = sys.modules[__name__]
requiredParamters= "logging, dryRun, authenticationMode, gatewayPrefix, tenantname, mindSphereEnvironment"
config.setSimpleConfigParametersToModule(thisModule, requiredParamters)

# Issue with that: 
# Pylint will complain, because it doesnt get, that those parameters are available
# So we that complaint will be one only time on top here:

logging = logging
dryRun = dryRun
authenticationMode = authenticationMode
gatewayPrefix = gatewayPrefix
tenantname = tenantname
###########

expiryTimeForSession = 120
expiryTimeForAppCredentials = 30

# If this is set to false, the user won't have to provide an XRSF Token (it will be randomized):
askForXSRFToken = False 

# Maybe new Scopes have to been added here in the future...
browserScopeDefinitions = {
"assetmanager": {"nameOfTool":"ASSET","applicationPrefix" : "assetmanager" , "stepsToGetIt": f"Login to MindSphere in your browser, open asset-manager, open development console (e.g. F12 in Chrome), change to 'Application'-Tab and extract corresponding cookie values from there."},
"uipluginmcnano": {"nameOfTool":"MindConnect NANOBOX", "applicationPrefix" : "uipluginassetmanagermcnano", "stepsToGetIt": "Login to MindSphere in your browser, open asset-manager, open an arbitrary MindConnect Nanobox agent's config, open development console (e.g. F12 in Chrome), change to 'Application'-Tab and extract corresponding cookie values from there."},
"uipluginmciot2040": {"nameOfTool":"MindConnect IOT2040","applicationPrefix" : "uipluginassetmanagermciot2040" , "stepsToGetIt": "Login to MindSphere in your browser, open asset-manager, open an arbitrary MindConnect IOT204 agent's config, open development console (e.g. F12 in Chrome), change to 'Application'-Tab and extract corresponding cookie values from there."},
"uipluginmclib": {"nameOfTool":"MindConnect LIBRARY", "applicationPrefix": "uipluginassetmanagermclib", "stepsToGetIt": "Login to MindSphere in your browser, open asset-manager, open an arbitrary MindConnect Lib agent's config, open development console (e.g. F12 in Chrome), change to 'Application'-Tab and extract corresponding cookie values from there."},
"uipluginIndustrialEdge": {"nameOfTool":"Industiral Edge Plugin", "applicationPrefix": f"edgeassetconfig-edgetest", "stepsToGetIt": "Login to MindSphere in your browser, open asset-manager, open an arbitrary Industrial Edge agent's config, open development console (e.g. F12 in Chrome), change to 'Application'-Tab and extract corresponding cookie values from there."},
}

#This regex is not used anymore 
# regex to find the underlying root mindsphere api in an URL
#templateFindRootApi =re.compile(r"(\/api\/.*\/v[\d]*)\/")

# Those headers will be added to most APIcalls. Maybe this is not good :)
additionalStandardHeadersForApiCalls = {'content-type': 'application/json','accept-encoding': 'gzip, deflate'}

sessionTokenFile =os.path.join (".","temp",tenantname + '_savedScopes.txt')

# Lookup Dict for API to application mapping in browser mode:
lookupDictApplicationScopeForApi= {
   "/api/mindconnectdevicemanagement/v3":                          "plugin",
   "/api/assetmanagement/v3":                                      "assetmanager",
   "/api/agentmanagement/v3":                                      "assetmanager",
   "/api/mindconnect/v3":                                          "plugin",
   "/api/agentmanagement/v3/agents":                               "plugin",
   "/api/agentmanagement/v3/agents?page=":                         "assetmanager",
   
  

}

agentTypeToPluginMapping = {
    "core.mcnano":      "uipluginmcnano",
    "core.mciot2040":   "uipluginmciot2040",
    "core.mclib":       "uipluginmclib",
    "core.industrialEdge" : "uipluginIndustrialEdge",
    f"{tenantname}.IndustrialEdgeWithTimeseries": "uipluginIndustrialEdge"

}


defaultAgentAssetType = 'core.mcnano'




#######################################
######### HELPER FUNCTIONS ############
#######################################

class ScopeNotFound(Exception):
    def __init__(self, assetType):
        self.assetType = assetType



#Modfiy serilization behaviour of json encoder for timestemps
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
    
        if isinstance(o, datetime):
            return o.isoformat()

        return super().default(o)

#######################################
class Empty:
    pass        # empty class definition

def lookupApplicationScopeForApi(originalUrl, assetType = defaultAgentAssetType):

    #specialCase -> dataSourceConfiguration is apparently only availalbe with plugin Scope

    numberOfMatchingChracters = 0
    rootApi = None
    applicationScope = None
    for key in lookupDictApplicationScopeForApi:
        if key in originalUrl:
            currentNumberOfMatchingChracters = len(key)
            if currentNumberOfMatchingChracters >= numberOfMatchingChracters:
                rootApi = key
                applicationScope = lookupDictApplicationScopeForApi[key]

    if not rootApi:
        print("No MindSphere API has been found within this url: \n{}".format(url))
        print("I am sorry for your loss...")
        exit(0)

    if applicationScope == "plugin":
        #Which plugin is used here?
        if assetType in agentTypeToPluginMapping:
            applicationScope = agentTypeToPluginMapping[assetType]
        else:
            print("Unkown assetType '{}' for scope detection with URL '{}'. Using default '{}' instead ".format(assetType,originalUrl, defaultAgentAssetType))
            applicationScope = agentTypeToPluginMapping[defaultAgentAssetType]
            #raise ScopeNotFound(assetType)

    return applicationScope, rootApi



#######################################



def mergeDicts(x, y):
    if y:
        z = x.copy()   
        z.update(y)   
        return z
    else:
        return x


#######################################

def askUserForBrowserCookies(browserScope, expired = False):
    from modules.mindsphereDataModelManager import MindsphereDataModelManager
    if not browserScope in browserScopeDefinitions:
        print("Unkown Scope for Browser Sessions {}. Please adapt scope Handler".format(browserScope))
        exit(0)

    if expired:
        print("Browser Session Token expired, please provide new session")

    else:
        print("You have chosen Browser Session Token Mode. \nPlease provide valid session data in the command line.")
        print("The importer assumes that your cookies are 'fresh' and valid\n")
        print("If your import process takes a long time you will have to refresh the browser session using F5 in Browser and reenter the session cookie data.\n")

    nameOfTool = browserScopeDefinitions[browserScope]["nameOfTool"] + ' Management'
    print(f"{'Scope needed for':<20} --> {nameOfTool :^35} <-- \n{'MindSphere-Tenant':<20} --> {tenantname.upper():^35} <--")
    print("{}".format(browserScopeDefinitions[browserScope]["stepsToGetIt"]))

    if browserScope == "assetmanager":
        print(f"https://{tenantname}-assetmanager.{mindSphereEnvironment}.mindsphere.io")
    else:
        tenantId = MindsphereDataModelManager.tenantId
        if browserScope == "uipluginmcnano" and MindsphereDataModelManager.randomNanoAgentId:
            
            entityId = MindsphereDataModelManager.randomNanoAgentId
            print(f"https://{tenantname}-assetmanager.{mindSphereEnvironment}.mindsphere.io/entity/{entityId}/plugin/uipluginassetmanagermcnano")

        if browserScope == "uipluginmciot2040" and MindsphereDataModelManager.randomIot2040AgentId:
            entityId = MindsphereDataModelManager.randomIot2040AgentId
            print(f"https://{tenantname}-assetmanager.{mindSphereEnvironment}.mindsphere.io/entity/{entityId}/plugin/uipluginassetmanagermciot2040")

        if browserScope == "uipluginmclib" and MindsphereDataModelManager.randomLibAgentId:
            entityId = MindsphereDataModelManager.randomLibAgentId
            print(f"https://{tenantname}-assetmanager.{mindSphereEnvironment}.mindsphere.io/entity/{entityId}/plugin/uipluginassetmanagermclib")           

    sessionId = input("SESSION:\n").strip()

    if askForXSRFToken:
        xrsfToken = input("XSRF-TOKEN:\n").strip()
    else:
        xrsfToken = str(uuid.uuid4())

    return sessionId, xrsfToken

#######################################

def isStillValid(creationTimestamp, expiryTimeInMinutes):
    now = datetime.now()
    if creationTimestamp != None:
            if isinstance(creationTimestamp,str):
                creationTimestamp = datetime.fromisoformat(creationTimestamp)
            difference = now - creationTimestamp
            
            if difference.seconds < expiryTimeInMinutes * 60:
                    if DEBUG:
                        print("DEBUG: Current AccessMethod still valid.")
                    return True
            else:
                return False
    else:
        print("Houston, we have no creationTimestamp for current SessionScope. This should not happen.")
        exit(-1)

#######################################

def generateToken():
    newToken = None
    if authenticationMode == "appCredentials":
        #Read App Credentials
        try:
            hostTenant = config.host_tenant
            userTenant = config.user_tenant
            clientSecret = config.client_secret
            clientID =config.client_id
            appversion = config.appversion
            appname = config.appname

        except:
            print ("Failed reading App Credentials-Section from Config - correct the relevant config-section or change the 'authenticationMode'.")
            print("Exiting now...")
            traceback.print_exc()
            exit(0)
    
        bodyAsJson = \
                {
                "appName": appname,
                "appVersion": appversion,
                "hostTenant": hostTenant,
                "userTenant": userTenant
                }
        headers = \
                {
                'content-type': 'application/json', 
                "X-SPACE-AUTH-KEY": 'Basic ' + base64.b64encode(bytes(clientID + ":" + clientSecret, 'utf-8')).decode('utf8')
                }

        response = ""

        try:
            response = requests.post(gatewayPrefix + "/api/technicaltokenmanager/v3/oauth/token", headers = headers, json=bodyAsJson)

            if logging in ("VERBOSE") : 
                "Response from -> Get Token via AppCredentials:"
                printRequestInformation(response)


            # Json Anwort umwandeln in Dictionary 
            json_data = json.loads(response.text)
            # Relevanten Parameter aus Dictionary auslesen
            newToken = json_data["access_token"]

        except Exception:
            traceback.print_exc()
            exit(-1)
            return -1      

    elif authenticationMode == "serviceCredentials":

        #Read Service Credentials
        try:
            serviceCredentialsClientID =  config.client_id
            serviceCredentialsClientSecret =  config.client_secret
            serviceCredentialsOAuthUrl = config.oAuthUrl
        except:
            print ("Failed reading Service Credentials-Section within Config - correct the relevant config-section or change 'authenticationMode'.")
            print("Exiting now...")
            traceback.print_exc()
            exit(0)
        bodyAsJson = {}
        
        headers = \
                {
                "Authorization": 'Basic ' + base64.b64encode(bytes(serviceCredentialsClientID + ":" + serviceCredentialsClientSecret, 'utf-8')).decode('utf8'),
                }
        response = ""
        
        try:
            
            response = requests.post(serviceCredentialsOAuthUrl+"?"+"grant_type=client_credentials", headers = headers, json=bodyAsJson)

            if logging in ("VERBOSE") : 
                print("Response from -> Get Token via ServiceCredentials:")
                printRequestInformation(response)

            # Json Anwort umwandeln in Dictionary 
            json_data = json.loads(response.text)
            # Relevanten Parameter aus Dictionary auslesen
            newToken = json_data["access_token"]

        except Exception:
            traceback.print_exc()
            exit(-1)
            return -1 

    elif authenticationMode == "manual":
            print("\n\n\n!!!! ATTENTION !!!!")
            print("You have chosen manual Token Mode. \nPlease provide a valid token in the command line.")
            print("The importer assumes that your token is 'fresh' and valid for 30 min - starting the moment you entered the token")
            print("If your import process takes a long time you will have to enter another token after those 30 min.")
            newToken = input("TokenInput:\n")
    else:
        print("The used Authentication Mode: '{}' is unkown. Please use a known one".format(authenticationMode))
        exit(-1)

    if newToken:
        return newToken
    else:
        print("Something went wrong with generating a token... at least there is none")
        print("This should not happen :) - please review the {}-Module".format(thisModule))
        exit(-1)

#######################################

#######################################


#######################################
############ MAIN BLOCK ###############
#######################################

currentTokenTimestamp = None
currentToken = None
xrsfToken = None
sessionId = None

copyOfRequestToExtractToken = None
    
lastToken = None
#Config einlesen


class ScopeAndSessionHandler():
  
    def __init__(self):
        
        #Load Session/Token Information from file if available (to avoid issuing a new one all the time)
        if os.path.isfile(sessionTokenFile):
            try:
                with open(sessionTokenFile, 'r') as tokenFile:  
                    self.scopeDatabase = json.load(tokenFile)
            except:

                print ("'Error reading {} - is this file corrupted? \n Mitigation: Delete the file, so that it will be recreated.\n".format(tokenFile))
                print("Exiting now...")
                traceback.print_exc()
                exit(-1)

        else:
            self.scopeDatabase = {}
                ## This dictionary will be a dictionary of the various-scope dicitionaries. 
                # On the root layer it will have entries like:
                # token -> this is a special mode
                # assetmanager
                # uipluginmcnano
                # uipluginmciot204
                # uipluginmclib

    def deleteSession(self, url, assetType):

        neededScope, rootAPI = lookupApplicationScopeForApi(url,assetType=assetType)
        del self.scopeDatabase[neededScope]
        with open(sessionTokenFile, 'w') as tokenFile:  
            json.dump(self.scopeDatabase, cls=DateTimeEncoder, fp=tokenFile)
            

    def generateScope(self, scope, expired = False):

        if authenticationMode == "browserSession":
            
            sessionId,xrsfToken = askUserForBrowserCookies(scope, expired )
            self.scopeDatabase[scope] = {
            "xrsfToken" : xrsfToken,
            "sessionId" : sessionId, 
            "applicationPrefix": browserScopeDefinitions[scope]["applicationPrefix"],
            "creationDate": datetime.now(),
            "expiryTimeInMinutes" : expiryTimeForSession
            }

        else:
            newToken = generateToken()
            self.scopeDatabase[scope]  = {
            "tokenValue" : newToken,
            "creationDate": datetime.now(),
            "expiryTimeInMinutes" : expiryTimeForAppCredentials
            }

        # Save current state to file each time a scope is (re-)generated
        with open(sessionTokenFile, 'w') as tokenFile:  
            json.dump(self.scopeDatabase, cls=DateTimeEncoder, fp=tokenFile)

    def getValidScope(self, scope):
        if scope in self.scopeDatabase:
            currentScope = self.scopeDatabase[scope]

            if isStillValid(currentScope["creationDate"], currentScope["expiryTimeInMinutes"]):
                return currentScope
            else:
                oldSessionId = currentScope["sessionId"]
                print(f"The old session ID was:\n{oldSessionId}")
                self.generateScope(scope, expired = True)
                return self.scopeDatabase[scope]
        else:
            self.generateScope(scope)
            return self.scopeDatabase[scope]
            

    def transformApiCall(self,url,assetType=None):

        #this function returns a modified URL and session parameters or a token:
        additionalHeaders = {}
        additionalCookies = {}

        """     if "https://" in url: #this branch is necessary, to differ from URL automatically generated through the "next links" repsonse within API calls
            # should there already be an 'http://'-string before the API, probably no transformation is needed... well
            newUrl = url

        else: """
        if authenticationMode == "browserSession":

            # in browser Session Mode: 
            # Look up the necessary application-prefix associated with the API call:
  
                neededScope, rootAPI = lookupApplicationScopeForApi(url,assetType=assetType)

                currentScope = self.getValidScope(neededScope)

                applicationPrefix = currentScope["applicationPrefix"] # Well, yes.. this is implementated in a very strange way...
                urlRemainder = url.split(rootAPI)[1]

                newUrl = "https://" + tenantname + "-" + applicationPrefix +  gatewayPrefix.replace("https://gateway",'') + rootAPI + urlRemainder
            
                additionalHeaders = {"X-XSRF-TOKEN":currentScope["xrsfToken"] } # des Ding kann man vermutlich immer setzen
                domain = "." + tenantname + "-" + applicationPrefix + rootAPI
                additionalCookies["XSRF-TOKEN"] ="{}; path=/; domain={};".format(currentScope["xrsfToken"], domain) 
                additionalCookies["SESSION"] ="{}; path=/; domain={};".format(currentScope["sessionId"], domain)

        else: #in token-mode just return the call with the leading gateway adress:
            
            currentScope = self.getValidScope("token")

            if "https://" in url:  #This is always the case, if a nextPage Link is used. There a cleaner ways to deal with this, but well...
                newUrl = url
            else:
                newUrl = gatewayPrefix + url
            additionalHeaders = {
                "Authorization": 'Bearer ' + currentScope["tokenValue"],
            }

        if DEBUG:
            print("Original URL:\n{}".format(url))
            print("Transformed URL:\n{}".format(newUrl))
        return newUrl, additionalHeaders, additionalCookies




scopeAndSessionHandler = ScopeAndSessionHandler()


def printRequestInformation(response):
    print('#'*60)
    print("######## An Https-Request has been issued ########")
    print(response)
    print("Headers of request: {}".format(response.request.headers))
    print("Body of request: {}".format(response.request.body))
    print("Url of request: {}".format(response.request.url))
    print("Response Status Code: {}".format(response.status_code))
    print("Respone Text: {}".format(response.text))
    print('#'*60)

    ''' Possible Response-Attributes:  
        'apparent_encoding', 'close', 'connection', 'content', 
        'cookies', 'elapsed', 'encoding', 'headers', 'history', 
        'is_permanent_redirect', 'is_redirect', 'iter_content', 'iter_lines', 
        'json', 'links', 'next', 'ok', 'raise_for_status', 'raw', 'reason', 'request', 
        'status_code', 'text', 'url' '''

    ''' Possible Response.request-Attributes: 
    'body', 'copy', 'deregister_hook', 'headers', 'hooks', 
    'method', 'path_url', 'prepare', 'prepare_auth', 'prepare_body', 
    'prepare_content_length', 'prepare_cookies', 'prepare_headers', 
    'prepare_hooks', 'prepare_method', 'prepare_url', 'register_hook', 'url' '''
                       




def wrapApiCall(url, requesttype, bodyAsJson = {}, standardHeaders = additionalStandardHeadersForApiCalls , files = {}, params = {}, data = {}, assetTypeToDeriveApplicationScope = None, additionalHeaders = None):

    headers = standardHeaders
    headers = mergeDicts(headers,additionalHeaders)
    cookies = {}          
    processThisRequestOneMoreTime = True
    sessionFailedCounter = 0
    retryCounter = 0

    while(processThisRequestOneMoreTime == True):
        if sessionFailedCounter > 5:
            print("Aborting this call after 5 failures with session cookies...\n")

            return {"result":None, "responseStatusCode":None, "responseText":None}
        retryCounter += 1
        if dryRun == False:   
            
            url, additionalHeaders, additionalCookies = scopeAndSessionHandler.transformApiCall(url = url, assetType = assetTypeToDeriveApplicationScope)
            headers = mergeDicts(headers,additionalHeaders)
            cookies = mergeDicts(cookies,additionalCookies)

            if DEBUG or config.askBeforeEachApiCall:
                print("*"*40) 
                print("URL FOR REQUEST:\n", url)
                print("-"*10) 
                print("REQUESTTYPE: ", requesttype)
                print("-"*10) 
                print("BODY OF REQUEST:")
                print(bodyAsJson)
                print("*"*40) 

            if config.askBeforeEachApiCall:

                if not config.yesOrNo("Do you want to really issue this request?"):
                    exit(0)

            if requesttype == "POST":
                    response = requests.post(url, headers = headers, json = bodyAsJson, params = params, data = data, files = files, cookies = cookies)
            if requesttype == "PUT":
                    response = requests.put(url, headers = headers, json=bodyAsJson, cookies = cookies)
            if requesttype == "GET":
                    response = requests.get(url, headers = headers, json=bodyAsJson, cookies = cookies)
            if requesttype == "DELETE":
                    response = requests.delete(url, headers = headers, json=bodyAsJson, cookies = cookies)


            if response.status_code >= 200 and response.status_code <300:
                processThisRequestOneMoreTime = False

            elif retryCounter > 1:
                processThisRequestOneMoreTime = False
                print(f"Retrying also failed. Aborting this call and printing what has beend tried to import...\n")
                print(url)
                print("!"*20)  
                print(requesttype)
                print("!"*20)  
                print(bodyAsJson)
                print("!"*80)  
                pprint.pprint(response)
                if config.autoContinueWhenApiCallFailed == False:
                    if not config.yesOrNo("Failed Request: Do you want to continue this batch?"):
                        exit(0)
            else:
                print(f"Request failed:\n\tURL: '{url}'\n\tAssetType for Scope Detection: '{assetTypeToDeriveApplicationScope}'")
                print(response.status_code)
                print(response.text)
                print(response.content)
                print("Retrying ... \n")
                
            if logging in ("VERBOSE") : 
                printRequestInformation(response)

            json_data = "Empty Response"
        
            if response.text not in ("", None): #Some requests provide no response text at all
                # TODO Hier noch den case abfangen, das herausfilter, wenn ein session cookei expired ist und dafür den fixen expiry-timer rausnehmen bzw deutlich höher setzen(auf 12h)

                if response.text.find("Since your browser does not support JavaScript") != -1:
                    print("\n!! ATTENTION !!: \nYour Session Cookie seems to not work properly. Please get the correct value for the Session Cookie and maybe refresh browser session")
                    print(f"This issue occured when trying to access {url}")
                    scopeAndSessionHandler.deleteSession(url = url, assetType = assetTypeToDeriveApplicationScope)
                    sessionFailedCounter +=1
                    processThisRequestOneMoreTime=True
                    retryCounter = 0
                    continue
                if response.text.find("is not equal to the Client id") != -1:
                    print(response.text)
                    print("\n!! ATTENTION !!: \nYour Session Cookie seems to not work properly. Please get the fitting scope from the correct browser application/plugin")
                    print(f"This issue occured when trying to access {url}")
                    scopeAndSessionHandler.deleteSession(url = url, assetType = assetTypeToDeriveApplicationScope)
                    processThisRequestOneMoreTime=True
                    sessionFailedCounter +=1
                    retryCounter = 0
                    continue

                response.encoding = response.apparent_encoding
                json_data = json.loads(response.text)

                
                # Parse JSON int
                # o a construct of dicts and list of dicts

        else: #Dry Run active
                processThisRequestOneMoreTime = False
                print("!"*80) 
                print ("!!!!!!!!!!!! ATENTION DRY RUN ACTIVATED - NO API CALLS WILL BE EXECUTED !!!!!!!!!!!!") 
                print("!"*20)  
                print(url)
                print("!"*20)  
                print(requesttype)
                print("!"*20)  
                print(bodyAsJson)
                print("!"*80)  
                response = Empty()
                response.status_code = 999
                bodyAsJson['THIS IS A FAKE REQUEST'] ='Request faked, since API Imports are currently disabled, because parameter dryRun is set in config'
            
                response.text =  str(bodyAsJson).replace("'",'"')

                request = Empty()
                request.headers = headers 
                request.body = bodyAsJson
                request.url = url
                response.request = request
                response.text = "Fake request has been issued"
                json_data = response.text

    # # Parse JSON into an actual object (instead of a list of dictonaries): this is not used, because SimpleNameSpace objects are not serializable
    # json_data = json.loads(response.content, object_hook=lambda d: SimpleNamespace(**d))


    return {"result":json_data, "responseStatusCode":response.status_code, "responseText":response.text}


def setOriginalRequest(request):
    global copyOfRequestToExtractToken
    copyOfRequestToExtractToken = request