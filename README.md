# dataModelHandler

This is an application that allows to import, export and delete MindSphere data models using csv-files as exchange format. <br>
Additionally mass agent creation, deriving of device-assets and mapping is fully supported.<br>
<br>
For now, the mode of operation has to be specified for a script run using the config-file or during execution<br>
The four possible modes are: import, export, delete, agents
<br>
# How to use it
This is a **Python 3.x** application. To start it  make sure that a valid configuration file is sitting in the root folder and then just run main.py<br>E.g. _python main.py_
<br>You might need to install the requests module previously using e.g. _pip install requests_ <br>
# Example
An example for a configuration file can be found in the folder<br> */inputFiles/MassOnboardingExample/config-MassOnboardingExample.ini*<br>
<br> Additionally  a short "How To" is sitting within the same folder explaining how to use that example:<br>*/inputFiles/MassOnboardingExample/_HowToUseThisExample.txt*


