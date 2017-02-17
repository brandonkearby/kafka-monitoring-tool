## Project Description
This is an application to track the offsets for the new kafka consumer. The app provides a REST endpoint that can be called on-demand to get this information. 
 
## Prerequisites
Java 1.7
Maven 3

## Installation (install commands)
git clone https://github.com/brandonkearby/kafka-monitoring-tool.git

cd kafka-monitor

mvn package

You will have the kafka-monitoring-tool-0.0.2.jar created in the “target” directory. 

## Running project
_java -jar target/kafka-monitoring-tool-0.0.2.jar server_

By default the application assumes the bootstrapServer is running localhost on port 9092. If you need to provide a zookeeper host, pass it as a jvm parameter like this:

_java -Ddw.bootstrapServer=<host:port> -jar kafka-monitoring-tool-0.0.2.jar server_

Once the server is up, run the following command from localhost to get the information in a json format.
_curl -X GET http://localhost:8080/kafka/offset_

## Configuration (config commands)
The application can also be passed a yml file as a configuration file, instead of passing the zookeeper urls on the command line. Default.yml file is available in the project. The way you start the project with yml file is like this:

_java -jar kafka-monitoring-tool-0.0.2.jar server default.yml_

### API parameters
There are few Query Params you can pass to the API to get specific results. Examples are:

If you would like the output in a HTML format instead of Json format, try this:
_http://localhost:8080/kafka/offset?outputType=html_


You can also configure all these properties in a yml config file and pass in the config file to the app, like
_java -jar kafka-monitoring-tool-0.0.2.jar server defaults.yml_
