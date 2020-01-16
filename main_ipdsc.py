from flask import Flask, request
import json, re
from elasticsearch import Elasticsearch
from configparser import ConfigParser
from datetime import datetime
from dateutil import tz
from controller import controller

__author__ = "Syahrul Al-Rasyid"


class ipdsc:
    def __init__(self):
        self.parser = ConfigParser()
        self.parser.read("config.conf")
        self.from_zone = tz.tzutc()
        self.to_zone = tz.tzlocal()
    
    def ipdsc_facebook(self):
        ip = self.parser.get('elastic', 'ipd')
        es = Elasticsearch(ip, port=9200)
        if 'user_id' in request.args:
            userid   = request.args['user_id']
            query = es.search(index='ipd-criteria-facebook-post-*', body={
                "sort": [
                    {
                    "created_at": {
                        "order": "desc"
                    }
                    }
                ],
                "query": {
                    "bool": {
                    "must": [
                        {
                        "match": {
                            "page_id": "{}".format(userid)
                        }
                        }
                    ]
                    }
                },
                "size": 1
            })
            print query
            if query['hits']['total'] == 0 :
                last_update = bool(False)
                source  = "not found"
                last_data_stream = bool(False)
            else:
                _source = query['hits']['hits'][0]['_source']
                source = _source['user_full_name']
                last_data_stream = "https://fb.com/{}".format(_source['id'])
                last    = _source['created_at']
                lasted  = re.sub('\+[^\s]+','',last)
                utc_to_local = datetime.strptime(lasted, '%Y-%m-%d %H:%M:%S')
                utc_to_local = utc_to_local.replace(tzinfo=self.from_zone)
                last_update = str(utc_to_local.astimezone(self.to_zone))
            jsons = {"source": source, "last_update": last_update, "last_data_stream": last_data_stream}
            if 'date' in request.args:
                date    = request.args['date']
                spliter = str(date).split('-')
                if spliter.__len__() == 2:
                    day = controller().getdays(int(spliter[0]),int(spliter[1]))
                    getpost = es.search(index='ipd-criteria-facebook-post-*', body={
                        "sort": [
                            {
                            "created_at": {
                                "order": "desc"
                            }
                            }
                        ], 
                        "query": {
                            "bool": {
                            "must": [
                                {
                                "match": {
                                    "page_id": "{}".format(userid)
                                }
                                },
                                {
                                "range": {
                                    "created_at": {
                                        "gte": "{}-01 00:00:00".format(date),
                                        "lte": "{0}-{1} 23:59:59".format(date,day),
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
                                "field": "type"
                            }
                            }
                        }, 
                        "size": 0
                    })
                    getcomment = es.search(index='ipd-criteria-facebook-comment-*', body={
                        "sort": [
                            {
                            "created_at": {
                                "order": "desc"
                            }
                            }
                        ], 
                        "query": {
                            "bool": {
                            "must": [
                                {
                                "match": {
                                    "page_id": "{}".format(userid)
                                }
                                },
                                {
                                "range": {
                                    "created_at": {
                                        "gte": "{}-01 00:00:00".format(date),
                                        "lte": "{0}-{1} 23:59:59".format(date,day),
                                        "format": "yyyy-MM-dd HH:mm:ss",
                                        "time_zone": "+07:00"
                                    }
                                }
                                }
                            ]
                            }
                        }, 
                        "size": 0
                    })
                elif spliter.__len__() == 3:
                    getpost = es.search(index='ipd-criteria-facebook-post-*', body={
                        "sort": [
                            {
                            "created_at": {
                                "order": "desc"
                            }
                            }
                        ], 
                        "query": {
                            "bool": {
                            "must": [
                                {
                                "match": {
                                    "page_id": "{}".format(userid)
                                }
                                },
                                {
                                "range": {
                                    "created_at": {
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
                                "field": "type"
                            }
                            }
                        }, 
                        "size": 0
                    })
                    getcomment = es.search(index='ipd-criteria-facebook-comment-*', body={
                        "sort": [
                            {
                            "created_at": {
                                "order": "desc"
                            }
                            }
                        ], 
                        "query": {
                            "bool": {
                            "must": [
                                {
                                "match": {
                                    "page_id": "{}".format(userid)
                                }
                                },
                                {
                                "range": {
                                    "created_at": {
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
                        "size": 0
                    })
                else:
                    getcomment = False
                    getpost = False
                totalcom    = getcomment['hits']['total']
                totalpost  = getpost['hits']['total']
                postdetails = controller().getdetails(getpost)
                jsons['Data'] = {'post_total' : totalpost, 'comment' : totalcom}
                if 'details' in request.args: jsons['Data']['post_details'] = postdetails
            result = json.dumps(jsons)
            return result
        else:
            return 'No parameter'

    def ipdsc_twitter(self):
        ip  = self.parser.get('elastic', 'ipd')
        es  = Elasticsearch(ip, port=9200)
        if 'username' in request.args:
            username = request.args['username']
            query = es.search(index='ipd-criteria-twitter-post-*',body={
                "sort": [
                    {
                    "created_at": {
                        "order": "desc"
                    }
                    }
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
            if query['hits']['total'] == 0 :
                last_update = bool(False)
                source = username
                last_data_stream = bool(False)
            else:
                _source = query['hits']['hits'][0]['_source']
                last    = _source['created_at']
                source = _source['username']
                last_data_stream = query['hits']['hits'][0]['_id']
                utc_to_local    = datetime.strptime(last, '%Y-%m-%d %H:%M:%S')
                utc_to_local    = utc_to_local.replace(tzinfo=self.from_zone)
                last_update = str(utc_to_local.astimezone(self.to_zone))
            jsons = {"source" : source, "last_update": last_update, "last_data_stream" : last_data_stream}
            if 'date' in request.args:
                date    = request.args['date']
                spliter = str(date).split('-')
                if spliter.__len__() == 2:
                    day = controller.getdays(int(spliter[0]), int(spliter[1]))
                    posttweet = es.search(index='ipd-criteria-twitter-post-*', body={
                        "query": {
                        "bool": {
                        "must": [
                            {
                            "match": {
                                "username": "{}".format(username)
                            }
                            },
                            {
                            "range":{
                                "created_at": {
                                "gte": "{}-01 00:00:00".format(date),
                                "lte": "{0}-{1} 23:59:59".format(date,day),
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
                                "field": "type"
                            }
                            }
                        }, 
                        "size": 0
                        })
                    postretweet = es.search(index='ipd-criteria-twitter-post-*', body={
                        "query": {
                            "bool": {
                            "should": [
                                {
                                "bool": {
                                    "must": [
                                    {
                                        "match": {
                                        "retweeted_user_screen_name": "{}".format(username)
                                        }
                                    },
                                    {
                                        "range": {
                                        "created_at": {
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
                                {
                                "bool": {
                                    "must": [
                                    {
                                        "match": {
                                        "mention": "{}".format(username)
                                        }
                                    },
                                    {
                                        "range": {
                                        "created_at": {
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
                                {
                                "bool": {
                                    "must": [
                                    {
                                        "match": {
                                        "in_quote_to_screen_name": "{}".format(username)
                                        }
                                    },
                                    {
                                        "range": {
                                        "created_at": {
                                            "gte": "{}-01 00:00:00".format(date),
                                            "lte": "{0}-{1} 23:59:59".format(date, day),
                                            "format": "yyyy-MM-dd HH:mm:ss",
                                            "time_zone": "+07:00"
                                        }
                                        }
                                    }
                                    ]
                                }
                                }
                            ]
                            }
                        }, 
                        "aggs": {
                            "type": {
                            "terms": {
                                "field": "type"
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
                    posttweet = es.search(index='ipd-criteria-twitter-post-*', body={
                        "query": {
                        "bool": {
                        "must": [
                            {
                            "match": {
                                "username": "{}".format(username)
                            }
                            },
                            {
                            "range":{
                                "created_at": {
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
                                "field": "type"
                            }
                            }
                        }, 
                        "size": 0
                        })
                    postretweet = es.search(index='ipd-criteria-twitter-post-*', body={
                        "query": {
                            "bool": {
                            "should": [
                                {
                                "bool": {
                                    "must": [
                                    {
                                        "match": {
                                        "retweeted_user_screen_name": "{}".format(username)
                                        }
                                    },
                                    {
                                        "range": {
                                        "created_at": {
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
                                {
                                "bool": {
                                    "must": [
                                    {
                                        "match": {
                                        "mention": "{}".format(username)
                                        }
                                    },
                                    {
                                        "range": {
                                        "created_at": {
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
                                {
                                "bool": {
                                    "must": [
                                    {
                                        "match": {
                                        "in_quote_to_screen_name": "{}".format(username)
                                        }
                                    },
                                    {
                                        "range": {
                                        "created_at": {
                                            "gte": "{} 00:00:00".format(date),
                                            "lte": "{} 23:59:59".format(date),
                                            "format": "yyyy-MM-dd HH:mm:ss",
                                            "time_zone": "+07:00"
                                        }
                                        }
                                    }
                                    ]
                                }
                                }
                            ]
                            }
                        }, 
                        "aggs": {
                            "type": {
                            "terms": {
                                "field": "type"
                            }
                            }
                        }, 
                        "size": 0
                    })
                    totaltweet = posttweet['hits']['total']
                    totalretweet = postretweet['hits']['total']
                    tweetdetails = controller().getdetails(posttweet)
                    retweetdetails = controller().getdetails(postretweet)
                jsons['Data'] = {}
                jsons['Data']['Tweet'] = {'Total': totaltweet}
                jsons['Data']['Retweet'] = {'Total': totalretweet}
                if 'details' in request.args:
                    jsons['Data']['Tweet']['details'] = tweetdetails
                    jsons['Data']['Retweet']['details'] = retweetdetails
            result = json.dumps(jsons)
            return result
        else:
            return 'No paraeter'
    
    def ipdsc_onlinenews(self):
        ip  = self.parser.get('elastic', 'ipd')
        es  = Elasticsearch(ip, port=9200)
        if 'media' in request.args:
            media_name = request.args['media']
            query = es.search(index='ipd-news-online-criteria-*', body={
            "sort": [
                {
                "created_at": {
                    "order": "desc"
                }
                }
            ], 
            "query": {
                "bool": {
                "must": [
                    {
                    "match": {
                        "source": "{}".format(str(media_name).lower().replace(' ','_'))
                    }
                    }
                ]
                }
            },
            "size": 1
            })
            if query['hits']['total'] == 0:
                last_update = bool(False)
                link    = bool(False)
                title   = bool(False)
                source = media_name
            else:
                _source = query['hits']['hits'][0]['_source']
                source  = _source['source']
                link    = _source['link']
                title   = _source['title']
                last = _source['created_at']
                utc_to_local = datetime.strptime(last, '%Y-%m-%dT%H:%M:%S')
                utc_to_local = utc_to_local.replace(tzinfo=self.from_zone)
                last_update = str(utc_to_local.astimezone(self.to_zone))
            jsons = {"source": source, "last_update": last_update, "last_data_stream": {"title":title,"link":link}}
            if 'date' in request.args:
                date = request.args['date']
                if 'offset' in request.args:
                    offset = request.args['offset']
                    post = es.search(index='ipd-news-online-criteria-*', body={
                    "sort": [
                        {
                        "created_at": {
                            "order": "desc"
                        }
                        }
                    ],
                    "query": {
                        "bool": {
                        "must": [
                            {
                            "match": {
                                "source": "{}".format(str(media_name).lower().replace(' ','_'))
                            }
                            },
                            {
                              "range": {
                                "created_at": {
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
                    "from": offset,
                    "size": 20
                    })
                else:
                    post = es.search(index='ipd-news-online-criteria-*', body={
                    "sort": [
                        {
                        "created_at": {
                            "order": "desc"
                        }
                        }
                    ],
                    "query": {
                        "bool": {
                        "must": [
                            {
                            "match": {
                                "source": "{}".format(str(media_name).lower().replace(' ','_'))
                            }
                            },
                            {
                            "range": {
                                "created_at": {
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
                    "from": 0,
                    "size": 20
                        })
                    offset = bool(False)
                total_post = post['hits']['total']
                jsons['post'] = {'start_offset':int(offset),'total_post': total_post, 'date': date}
                loop = 0
                if int(total_post) < 20: 
                    loop = total_post+loop
                else: 
                    loop = loop + 20
                postingan = []
                for a in range(loop):
                    _source = post['hits']['hits'][a]['_source']
                    news_id = _source['id']
                    pub     = _source['created_at']
                    utc_to_local_post = datetime.strptime(pub,'%Y-%m-%dT%H:%M:%S')
                    utc_to_local_post = utc_to_local_post.replace(tzinfo=self.from_zone)
                    pubdate = str(utc_to_local_post.astimezone(self.to_zone))
                    title   = _source['title']
                    url     = _source['link']
                    image   = _source['image']                    
                    try:
                        content = _source['ann_clean_text']
                        postingan.append({
                            'ann_clean_text':"""{}""".format(content),
                            'news_id':news_id,
                            'pubdate':pubdate,
                            'title' : title,
                            'link'  : url,
                            'image' : image
                        })
                    except :
                        content = _source['content']
                        postingan.append({
                            'content':"""{}""".format(content),
                            'news_id':news_id,
                            'pubdate':pubdate,
                            'title' : title,
                            'link'  : url,
                            'image' : image
                        })
                      
                jsons['post']['post'] = postingan
                
            result = json.dumps(jsons)                
            return result
        else:
            return 'No parameters'
                
