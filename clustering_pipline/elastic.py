from elasticsearch import Elasticsearch
import cPickle
import json


es = Elasticsearch(hosts = [{'host':'54.153.119.78', 'port':9200}])
result = es.search(index='nga', doc_type='build', body={"fields" : [
    "data.rootProjectName",
    "@timestamp",
    "message",
    "data.result",
    "data.projectName"],
  "query": {
    "and":[
      {
        "match_phrase":{
          "data.rootProjectName" : "MQM-job-Compile-Server"
        }
      },
      {
        "match": {
          "data.projectName":"master"
        }
      },
      {
        "match": {
          "data.buildVariables.BUILD_URL":"http://mydtbld0049.hpeswlab.net:8080"
        }
      },
      {
        "match": {
          "data.url":"quick"
        }
      }]
  },
"from":0,
"size":650
}
# , request_timeout = 3600
)

cPickle.dump(result, open('results_all.p', "wb"))
with open('results_all.json', 'w') as outfile:
    json.dump(result, outfile)