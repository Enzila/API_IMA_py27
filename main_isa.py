from flask import Flask, request
from flask_restplus import Api
import json
from elasticsearch import Elasticsearch
from configparser import ConfigParser
from datetime import datetime
from dateutil import tz

app = Flask(__name__)
api = Api(app)

parser  = ConfigParser()
parser.read("config.conf")
from_zone   = tz.tzutc()
to_zone = tz.tzlocal()

@app.route('/isa/onlinenews')
def isa_onlinenews():
    ip  = parser.get('elastic','isa')
    es  = Elasticsearch(ip, port=5245)
    if 'media' in request.args:
        media_name  = request.args['media']
        query   = es.search(index='online-news-isa-*',body = {
            "sort": [
                {"created_at": {"order": "desc"}}
            ],
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "source": "bengawan+pos"
                            }
                        }
                    ]
                }
            },
            "size": 1
        })
        # print query
        if query['hits']['total'] == 0:
            last_update = "null"
            link    = "null"
            source  = media_name
        else:
            _source = query['hits']['hits'][0]['_source']
            last_update = _source['created_at']
            source  = _source['source']
            link    = _source['link']

        jsons   = {"source":source,"last_update":last_update, "last_data_stream" : link}
        result  = json.dumps(jsons)
        return result
    else:
        return 'No parameters'


if __name__ == '__main__':
    # isa_onlinenews()
    app.run(port=5003)