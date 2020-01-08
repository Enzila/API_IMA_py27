from flask import Flask, request
import json, re
from elasticsearch import Elasticsearch
from configparser import ConfigParser
from datetime import datetime
from dateutil import tz
from controller import controller

class isa:
    def __init__(self):
        self.parser = ConfigParser()
        self.parser.read("config.conf")
        self.from_zone = tz.tzutc()
        self.to_zone = tz.tzlocal()

    def isa_twitter(self):
        ip = self.parser.get('elastic', 'isa')
        es = Elasticsearch(ip, port=5245)
        if 'username' in request.args:
            username = request.args['username']
            query = es.search(index='new-data-twitter-isa-*', body={
                "sort":
                    [
                        {"created_at": {"order": "desc"}}
                    ],
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "username": "{}".format(username)
                                }
                            }
                        ]
                    }
                },
                "size": 1
            })
            if query['hits']['total'] == 0:
                last_update = bool(False)
                source = username
                last_data_stream = bool(False)
            else:
                _source = query['hits']['hits'][0]['_source']
                last = _source['created_at']
                lasted = re.sub('\+[^\s]+', '', last)
                source = _source['username']
                last_data_stream = query['hits']['hits'][0]['_id']
                utc_to_local = datetime.strptime(lasted, '%a %b %d %H:%M:%S %Y')
                utc_to_local = utc_to_local.replace(tzinfo=self.from_zone)
                last_update = str(utc_to_local.astimezone(self.to_zone))
            jsons = {"source": source, "last_update": last_update, "last_data_stream": last_data_stream}
            if 'date' in request.args:
                date = request.args['date']
                spliter = str(date).split('-')
                if spliter.__len__() == 2:
                    day = controller().getdays(int(spliter[0]), int(spliter[1]))
                    posttweet = es.search(index='new-data-twitter-isa-*', body={
                        "query":
                            {
                                "bool":
                                    {
                                        "must":
                                            [
                                                {
                                                    "match": {
                                                        "username": "{}".format(username)
                                                    }
                                                },
                                                {
                                                    "range": {
                                                        "timestamp": {
                                                            "gte": "{}-01 00:00:00".format(date),
                                                            "lte": "{0}-{1} 23:59:59".format(date, day),
                                                            "format": "yyyy-MM-dd HH:mm:ss",
                                                            "time_zone": "+07:00"
                                                        }
                                                    }
                                                }
                                            ]
                                    }
                            },
                        "aggs": {
                            "type": {
                                "terms": {
                                    "field": "type.keyword"
                                }
                            }
                        },
                        "size": 0
                    })
                    postretweet = es.search(index='new-data-twitter-isa-*', body={
                        "query":
                            {
                                "bool":
                                    {
                                        "must":
                                            [
                                                {
                                                    "query_string": {
                                                        "query": "retweeted_user_screen_name:\"{0}\" OR mention : \"{0}\" OR in_quote_to_screen_name:\"{0}\"".format(
                                                            username),
                                                        "analyze_wildcard": "true",
                                                        "default_field": "*"
                                                    }
                                                },
                                                {
                                                    "range": {
                                                        "timestamp": {
                                                            "gte": "{}-01 00:00:00".format(date),
                                                            "lte": "{0}-{1} 23:59:59".format(date, day),
                                                            "format": "yyyy-MM-dd HH:mm:ss",
                                                            "time_zone": "+07:00"
                                                        }
                                                    }
                                                }
                                            ]
                                    }
                            },
                        "aggs": {
                            "type": {
                                "terms": {
                                    "field": "type.keyword",
                                    "size": 4
                                }
                            }
                        },
                        "size": 0
                    })
                    totaltweet = posttweet['hits']['total']
                    totalretweet = postretweet['hits']['total']
                    tweetdetails = controller().getdetails(posttweet)
                    retweetdetails = controller().getdetails(postretweet)
                elif spliter.__len__() == 3:
                    posttweet = es.search(index='new-data-twitter-isa-*', body={
                        "query":
                            {
                                "bool":
                                    {
                                        "must":
                                            [
                                                {
                                                    "match": {
                                                        "username": "{}".format(username)
                                                    }
                                                },
                                                {
                                                    "range": {
                                                        "timestamp": {
                                                            "gte": "{} 00:00:00".format(date),
                                                            "lte": "{} 23:59:59".format(date),
                                                            "format": "yyyy-MM-dd HH:mm:ss",
                                                            "time_zone": "+07:00"
                                                        }
                                                    }
                                                }
                                            ]
                                    }
                            },
                        "aggs": {
                            "type": {
                                "terms": {
                                    "field": "type.keyword"
                                }
                            }
                        },
                        "size": 0
                    })
                    postretweet = es.search(index='new-data-twitter-isa-*', body={
                        "query":
                            {
                                "bool":
                                    {
                                        "must":
                                            [
                                                {
                                                    "query_string": {
                                                        "query": "retweeted_user_screen_name:\"{0}\" OR mention : \"{0}\" OR in_quote_to_screen_name:\"{0}\"".format(
                                                            username),
                                                        "analyze_wildcard": "true",
                                                        "default_field": "*"
                                                    }
                                                },
                                                {
                                                    "range": {
                                                        "timestamp": {
                                                            "gte": "{} 00:00:00".format(date),
                                                            "lte": "{} 23:59:59".format(date),
                                                            "format": "yyyy-MM-dd HH:mm:ss",
                                                            "time_zone": "+07:00"
                                                        }
                                                    }
                                                }
                                            ]
                                    }
                            },
                        "aggs": {
                            "type": {
                                "terms": {
                                    "field": "type.keyword",
                                    "size": 4
                                }
                            }
                        },
                        "size": 0
                    })
                    totaltweet = posttweet['hits']['total']
                    totalretweet = postretweet['hits']['total']
                    tweetdetails = controller().getdetails(posttweet)
                    retweetdetails = controller().getdetails(postretweet)
                else:
                    totaltweet = False
                    tweetdetails = False
                    totalretweet = False
                    retweetdetails = False
                jsons['Data'] = {}
                jsons['Data']['Tweet'] = {'Total': totaltweet}
                jsons['Data']['Retweet'] = {'Total': totalretweet}
                if 'details' in request.args:
                    jsons['Data']['Tweet']['details'] = tweetdetails
                    jsons['Data']['Retweet']['details'] = retweetdetails
            result = json.dumps(jsons)
            return result
        else:
            return 'No parameter'

    def isa_onlinenews_test(self):
        ip  = self.parser.get('elastic','isa')
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