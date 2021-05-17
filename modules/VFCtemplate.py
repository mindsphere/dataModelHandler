template = r'[{"id":"8038b3a4.0683c","type":"create event","z":"87f1cf6f.a9805","name":"","asset":"","assetName":"","severity":"","acknowledged":"","source":"VFC","description":"","x":810,"y":760,"wires":[]},{"id":"bbeed8be.9f2558","type":"function","z":"87f1cf6f.a9805","name":"","func":"//AssetIDs of assets for the simulation\nvar machines = $assetList;\n                \n//Aspects to be fed with data within those asset \nvar aspects = $aspectList;\n\n//Datamodel used in the aspects and the simulator parameters\nvar data = $dataList\n\nvar eventDefinitions = {\n    \"e1\": {\n        \"source\":\"the great warning source  \",\n        \"description\": \"You need to have a look here soon\",\n        \"acknowledged\": false,\n        \"severity\": 30\n    },\n        \n\n    \"e2\": {\n        \"source\":\"the great error source 2\",\n        \"description\": \"Help! Someone might die! Do Something! \",\n        \"acknowledged\": false,\n        \"severity\": 20\n    }\n}\n\nlet currentCyclePositions = flow.get(\"currentCyclePositions\") || {}\n//Simulate data now...\nfor (var machine = 0; machine < machines.length; machine++){\n\n    for (var aspect = 0; aspect < aspects.length; aspect++){\n        \n        msg.topic = machines[machine] + (\"/\" + aspects[aspect])\n        let potentialResult = data[aspect]\n        for (var variable in potentialResult)\n            if (Array.isArray(potentialResult[variable]))\n            {   \n                var nextElement = getNextElement(machines[machine],aspects[aspect],variable,potentialResult[variable],currentCyclePositions)\n                if (nextElement.includes(\"(e\"))\n                {\n                    var splitted = nextElement.split(\"(\");\n                    nextElement = splitted[0]\n                    var eventType = splitted[1].split(\")\")[0];\n                    if (eventType in eventDefinitions)\n                        {\n                        msg.description = eventDefinitions[eventType][\"description\"]\n                        msg.asset = machines[machine]\n                        msg.acknowledged = eventDefinitions[eventType][\"acknowledged\"]\n                        msg.severity = eventDefinitions[eventType][\"severity\"]\n                        msg.payload = eventDefinitions[eventType][\"source\"]\n                        node.send([null,msg])\n                        }\n                }\n  \n                potentialResult[variable] = nextElement \n            }\n        msg.payload = potentialResult\n        msg._time = new Date();\n        \n        node.send([msg,null])\n\n        \n        \n    }\n}\nflow.set(\"currentCyclePositions\", currentCyclePositions);\n\n/******************************************************************************************/\n/**\n * Returns a random number between min (inclusive) and max (exclusive)\n */\nfunction getRandomDouble(min, max) {\n    return (Math.random() * (max - min) + min).toString();\n}\n\n/**\n * Returns a random integer between min (inclusive) and max (inclusive).\n * The value is no lower than min (or the next integer greater than min\n * if min is not an integer) and no greater than max (or the next integer\n * lower than max if max is not an integer).\n * Using Math.round() will give you a non-uniform distribution!\n */\nfunction getRandomInt(min, max) {\n    min = Math.ceil(min);\n    max = Math.floor(max);\n    return (Math.floor(Math.random() * (max - min + 1)) + min).toString();\n}\n\n/**\n * Returns a random boolean value with given likelihood in percent to be TRUE\n * getTrueBoolean(10) => returns 10% TRUE, 90% FALSE\n */\nfunction getTrueBoolean(TrueLikelihood) {\n    return (Math.random() < (TrueLikelihood / 100.0)).toString();\n}\n\nfunction getNextElement(machine,aspect,variable,dataList,currentCyclePositions) {\n    let objectName = machine + aspect + variable\n    let currentPositionInDataList = 0\n    if (objectName in currentCyclePositions)\n    {\n        currentPositionInDataList = currentCyclePositions[objectName] + 1\n    }\n    \n    if (currentPositionInDataList > dataList.length-1)\n    {\n        currentPositionInDataList = 0\n    }\n    // Write back old position\n    currentCyclePositions[objectName] = currentPositionInDataList\n    return dataList[currentPositionInDataList].toString()\n\n}\n\n/**\n * Returns a random boolean value with given likelihood in percent to be TRUE\n * getTrueBoolean(10) => returns 10% TRUE, 90% FALSE\n */\nfunction getTrueBoolean(TrueLikelihood) {\n    return (Math.random() < (TrueLikelihood / 100.0)).toString();\n}\n\n/******************************************************************************************/\n","outputs":"2","noerr":0,"x":620,"y":720,"wires":[["72d8b2fa.12762c"],["8038b3a4.0683c"]],"_type":"node"},{"id":"b5f4ca13.8dba18","type":"inject","z":"87f1cf6f.a9805","name":"","topic":"","payload":"","payloadType":"date","repeat":"","repeatEnd":"0","endTime":"0","crontab":"","once":false,"properties":"","timezone":"utc","betweentimesunit":"m","enableRuleEngine":false,"x":420,"y":720,"wires":[["bbeed8be.9f2558"]]},{"id":"72d8b2fa.12762c","type":"write timeseries","z":"87f1cf6f.a9805","name":"","topic":"","topicLabel":"","assetName":"","x":820,"y":720,"wires":[]}]'