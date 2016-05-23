Commands:
curl -XDELETE localhost:9200/_template/logstash
curl -XDELETE 'http://localhost:9200/diploma'
curl -XGET 'localhost:9200/diploma?pretty'
cat diploma/input_data/road-acc/DfTRoadSafety_Accidents_2014.csv | logstash-2.3.2/bin/logstash -f simple_map.conf
export PIG_CLASSPATH=/home/nik/wrk/elasticsearch-hadoop-2.3.2/dist/elasticsearch-hadoop-pig-2.3.2.jar 
pig -x mapreduce pig/scripts/use_cases/Acc.pig

