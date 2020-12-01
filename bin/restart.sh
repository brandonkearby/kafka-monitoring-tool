JAR=kafka-monitoring-tool-0.0.2.jar
jps | grep $JAR | awk '{print $1}' | xargs kill
sleep 2
PROJECT_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."
nohup java -jar $PROJECT_HOME/target/$JAR server $PROJECT_HOME/conf/kafka-monitoring-tool.yml >> /data/var/log/kafka-monitoring-tool/kafka-monitoring-tool.log &
