from flask import Flask, request
from flask_restplus import Api
import json,re,pymysql
from elasticsearch import Elasticsearch
from configparser import ConfigParser
from datetime import datetime
from dateutil import tz
from calendar import monthrange

__author__ = "Kenzila"

app = Flask(__name__)
api = Api(app)

parser = ConfigParser()
parser.read("config.conf")

from_zone = tz.tzutc()
to_zone = tz.tzlocal()

def getdetails(query):
    details = query['aggregations']['type']['buckets']
    detailsdict = {}
    for a in range(details.__len__()):
        key = details[a]['key']
        if details[a]['key'] == 'null':
            key = 'comment'
        val = details[a]['doc_count']
        detailsdict[key] = val
    return detailsdict

# def queryelastic(es,index,id,gte,lte,querystring=False,aggregation=False):
#     if querystring is True:
#         query = es.search(index=index, body={
#             "query":
#                 {
#                     "bool":
#                         {
#                             "must":
#                                 [
#                                     {
#                                         "query_string": {
#                                             "query": "retweeted_user_screen_name:\"{0}\" OR mention : \"{0}\" OR in_quote_to_screen_name:\"{0}\"".format(
#                                                 id),
#                                             "analyze_wildcard": "true",
#                                             "default_field": "*"
#                                         }
#                                     },
#                                     {
#                                         "range": {
#                                             "created_at": {
#                                                 "gte": "{} 00:00:00".format(gte),
#                                                 "lte": "{} 23:59:59".format(lte),
#                                                 "format": "yyyy-MM-dd HH:mm:ss",
#                                                 "time_zone": "+07:00"
#                                             }
#                                         }
#                                     }
#                                 ]
#                         }
#                 },
#             "aggs": {
#                 "type": {
#                     "terms": {
#                         "field": "type.keyword",
#                     }
#                 }
#             },
#             "size": 0
#         })


def getdays(tahun, bulan):
    return monthrange(tahun, bulan)[1]

@app.route('/ipd/onlinenews')
def ipd_onlinenews():
    ip = parser.get('elastic','ipd')
    es = Elasticsearch(ip, port=9200)
    if 'media' in request.args:
        media_name = request.args['media']
        query   = es.search(index='ipd-news-online-criteria-*', body={
            "sort": [
                {"created_at": {"order": "desc"}}
            ],
            "query": {
                "bool": {
                    "must": [
                        {
                        "match": {
                            "source": "{}".format(str(media_name).lower())
                            }
                        }
                    ]
                }
            },
            "size": 1
        })
        if query['hits']['total'] == 0 :
            last_update = bool(False)
            link    = bool(False)
            title   = bool(False)
            source = media_name
        else:
            _source = query['hits']['hits'][0]['_source']
            source  = _source['source']
            link    = _source['link']
            title   = _source['title']
            last_update = _source['pubdate']
        jsons = {"source": source, "last_update": last_update, "last_data_stream": {"title":title,"link":link}}
        if 'date' in request.args:
            date    = request.args['date']
            if 'offset' in request.args:
                offset = request.args['offset']
                post = es.search(index='ipd-news-online-criteria-*', body={
                    "sort": [
                        {"created_at": {"order": "desc"}}
                    ],
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match": {
                                        "source": "{}".format(str(media_name).lower())
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
                    "from": offset, "size": 20
                })
            else:
                post    = es.search(index='ipd-news-online-criteria-*',body = {
                "sort": [
                            {"created_at": {"order": "desc"}}
                        ],
                              "query": {
                                  "bool": {
                                      "must": [
                                          {
                                              "match": {
                                                  "source": "{}".format(str(media_name).lower())
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
                            "from": 0,"size": 20
                          })
                offset = bool(False)
            total_post = post['hits']['total']
            jsons['post']= {'start_offset':int(offset),'total_post':total_post,'date': date}
            if int(total_post) < 20: loop = total_post
            else: loop = 20
            postingan = []
            for a in range(loop):
                # print a
                try:
                    _source = post['hits']['hits'][a]['_source']

                    news_id = _source['id']
                    pubdate = _source['pubdate']
                    title   = _source['title']
                    url = _source['link']
                    image = _source['image']
                    content = _source['ann_clean_text']

                    # print postingan
                    postingan.append({
                                  'content':"""{}""".format(content),
                                  'news_id':news_id,
                                  'pubdate':pubdate,
                                  'title':title,
                                  'link':url,
                                  'image':image
                                  })
                except:
                    continue
                    # last_update = date
            jsons['post']['post']= postingan


        result = json.dumps(jsons)
        # print result
        return result

        # str(media_name).lower()
    else:
        return 'No parameters'

@app.route('/ipd/facebook')
def ipd_facebook():
    ip = parser.get('elastic', 'ipd')
    es = Elasticsearch(ip, port=9200)
    if 'user_id' in request.args:
        userid   = request.args['user_id']
        query = es.search(index='ipd-criteria-facebook-post-*', body={
  "sort" : [
        { "created_at" : {"order" : "desc"}}
    ],
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "user_id" : "{}".format(userid)
          }
        }
      ]
    }
  },
  "size": 1
})
        if query['hits']['total'] == 0:
            last_update = bool(False)
            source  = userid
            last_data_stream = bool(False)
        else:
            _source = query['hits']['hits'][0]['_source']
            source = _source['page_name']
            last_data_stream = _source['url']
            last = _source['created_at']
            print last
            utc_to_local = datetime.strptime(last,'%Y-%m-%d %H:%M:%S')
            utc_to_local = utc_to_local.replace(tzinfo=from_zone)
            last_update = str(utc_to_local.astimezone(to_zone))
        jsons = {"source": source, "last_update": last_update, "last_data_stream": last_data_stream}
        if 'date' in request.args:
            date = request.args['date']
            getcomment  = es.search(index='ipd-criteria-facebook-comment-*', body= {
                "query":
                    {
                        "bool":
                            {
                                "must":
                                    [
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
            getpost = es.search(index='ipd-criteria-facebook-post-*', body= {
                "query":
                  {
                    "bool":
                    {
                      "must":
                        [
                          {
                    "match": {
                      "user_id": "{}".format(userid)
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
                "size":0
            })
            postvideo = es.search(index='ipd-criteria-facebook-post-*', body= {
                "query":
                    {
                        "bool":
                            {
                                "must":
                                    [
                                        {
                                            "match": {
                                                "user_id": "{}".format(userid)
                                            }
                                        }, {
                                        "match": {
                                            "type": "video"

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
            totalpost = getpost['hits']['total']
            totalcomment = getcomment['hits']['total']
            # jsons['comment'] = totalcomment
            jsons['post'] = {'total_post': totalpost,
                             'total_comment': totalcomment}
            if 'details' in request.args:
                postphoto = es.search(index='ipd-criteria-facebook-post-*', body= {
                "query":
                    {
                        "bool":
                            {
                                "must":
                                    [
                                        {
                                            "match": {
                                                "user_id": "{}".format(userid)
                                            }
                                        }, {
                                        "match": {
                                            "type": "photo"

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
                postlink = es.search(index='ipd-criteria-facebook-post-*', body={
                "query":
                    {
                        "bool":
                            {
                                "must":
                                    [
                                        {
                                            "match": {
                                                "user_id": "{}".format(userid)
                                            }
                                        }, {
                                        "match": {
                                            "type": "link"

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
                poststatus = es.search(index='ipd-criteria-facebook-post-*', body={
                "query":
                    {
                        "bool":
                            {
                                "must":
                                    [
                                        {
                                            "match": {
                                                "user_id": "{}".format(userid)
                                            }
                                        }, {
                                        "match": {
                                            "type": "status"

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
                postevent = es.search(index='ipd-criteria-facebook-post-*', body={
                "query":
                    {
                        "bool":
                            {
                                "must":
                                    [
                                        {
                                            "match": {
                                                "user_id": "{}".format(userid)
                                            }
                                        }, {
                                        "match": {
                                            "type": "event"

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
                postalbum = es.search(index='ipd-criteria-facebook-post-*', body={
                "query":
                    {
                        "bool":
                            {
                                "must":
                                    [
                                        {
                                            "match": {
                                                "user_id": "{}".format(userid)
                                            }
                                        }, {
                                        "match": {
                                            "type": "album"

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
                postmusic = es.search(index='ipd-criteria-facebook-post-*', body={
                "query":
                    {
                        "bool":
                            {
                                "must":
                                    [
                                        {
                                            "match": {
                                                "user_id": "{}".format(userid)
                                            }
                                        }, {
                                        "match": {
                                            "type": "music"

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
                postnote = es.search(index='ipd-criteria-facebook-post-*', body={
                "query":
                    {
                        "bool":
                            {
                                "must":
                                    [
                                        {
                                            "match": {
                                                "user_id": "{}".format(userid)
                                            }
                                        }, {
                                        "match": {
                                            "type": "note"

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
                totalphoto = postphoto['hits']['total']
                totallink = postlink['hits']['total']
                totalstatus = poststatus['hits']['total']
                totalevent = postevent['hits']['total']
                totalalbum = postalbum['hits']['total']
                totalmusic = postmusic['hits']['total']
                totalnote = postnote['hits']['total']
                totalvideo = postvideo['hits']['total']
                jsons['post']['details'] = {
                    "photo": totalphoto,
                    "video": totalvideo,
                    "link": totallink,
                    "status": totalstatus,
                    "event": totalevent,
                    "album": totalalbum,
                    "music": totalmusic,
                    "note": totalnote
                }
        result  = json.dumps(jsons)
        return result
    else:
        return 'No parameter'

@app.route('/ipd/twitter')
def ipd_twitter():
    ip  = parser.get('elastic', 'ipd')
    es  = Elasticsearch(ip, port=9200)
    if 'username' in request.args:
        username    = request.args['username']
        query   = es.search(index='ipd-criteria-twitter-post-*',body= {
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
            utc_to_local    = utc_to_local.replace(tzinfo=from_zone)
            last_update = str(utc_to_local.astimezone(to_zone))
        jsons = {"source" : source, "last_update": last_update, "last_data_stream" : last_data_stream}
        if 'date' in request.args:
            date    = request.args['date']
            getpost = es.search(index='ipd-criteria-twitter-post-*', body={
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
            posttweet   = es.search(index='ipd-criteria-twitter-post-*', body={
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
                                            "match": {
                                                "type" : "tweet"
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
            postretweet = es.search(index='ipd-criteria-twitter-post-*', body=
            {
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
                                            "match": {
                                                "type": "retweet"
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
            postreply   = es.search(index='ipd-criteria-twitter-post-*', body={
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
                                            "match": {
                                                "type": "reply"
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
            postquoted = es.search(index='ipd-criteria-twitter-post-*', body={
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
                                            "match": {
                                                "type": "quoted"
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

            totalpost   = getpost['hits']['total']
            totaltweet  = posttweet['hits']['total']
            totalretweet  = postretweet['hits']['total']
            totalreply = postreply['hits']['total']
            totalquoted = postquoted['hits']['total']

            jsons['post'] = {'total_post' : totalpost}
            jsons['post']['details'] = {
                "tweet" : totaltweet,
                "retweet": totalretweet,
                "reply": totalreply,
                "quoted": totalquoted
            }

        result = json.dumps(jsons)
        return result
    else:
        return 'No parameter'

@app.route('/akun/twitter')
def akun_twitter():
    conek = pymysql.connect(
        host    = parser.get('database','sc_bintaro_tw_host'),
        user    = parser.get('database','sc_bintaro_tw_user'),
        password    = parser.get('database','sc_bintaro_tw_pass'),
        db  = parser.get('database','sc_bintaro_tw_db'),
        charset = 'utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    jsons = {'ket':'twitter_from_SC'}
    cursor = conek.cursor()
    queri = "SELECT `user_id`,`name`,`screen_name`,`add_date` FROM `twitter_account_track` ORDER BY `twitter_account_track`.`screen_name` ASC"
    cursor.execute(queri)
    data = cursor.fetchall()
    da_list = []
    for a in data:
        date = a['add_date']
        da_list.append( {'user_id': a['user_id'],
                         'screen_name': a['screen_name'],
                         'name' : a['name'],
                         'add_date' : str(date)})
    jsons['list'] = da_list
    result = json.dumps(jsons)
    return result

@app.route('/akun/facebook')
def akun_facebook():
    conek   = pymysql.connect(
        host    = parser.get('database','sc_ph_fb_host'),
        user    = parser.get('database','sc_ph_fb_user'),
        password    = parser.get('database','sc_ph_fb_pass'),
        db  = parser.get('database','sc_ph_fb_db'),
        charset = 'utf8mb4',
        cursorclass = pymysql.cursors.DictCursor
    )
    jsons   = {'ket' : 'facebook_from_SC'}
    cursor  = conek.cursor()
    queri   = "SELECT `fb_id`,`name`,`add_date` FROM `facebook_page_track` ORDER BY `facebook_page_track`.`name` ASC"
    cursor.execute(queri)
    data    = cursor.fetchall()
    da_list = []
    for a in data:
        date = a['add_date']
        da_list.append({'user_id' : a['fb_id'],
                        'name'  : a['name'],
                        'add_date'  : str(date)})
    jsons['list'] = da_list
    result = json.dumps(jsons)
    return result

@app.route('/isa/onlinenews')
def isa_onlinenews():
    ip  = parser.get('elastic','isa')
    es  = Elasticsearch(ip, port=5245)
    if 'media' in request.args:
        media_name  = request.args['media']
        query   = es.search(index='online-news-isa-*', body=
                    {
                        "sort": [
                            {"created_at": {"order": "desc"}}
                        ],
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "match": {
                                            "source.keyword": media_name
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
            source  = media_name
        else:
            _source = query['hits']['hits'][0]['_source']
            source  = _source['source']
            link    = _source['link']
            title   = _source['title']
            last    = _source['created_at']
            utc_to_local = datetime.strptime(last, '%Y-%m-%d %H:%M:%S')
            utc_to_local = utc_to_local.replace(tzinfo=from_zone)
            last_update = str(utc_to_local.astimezone(to_zone))
        jsons = {"source": source, "last_update": last_update, "last_data_stream": {"title": title, "link": link}}
        if 'date' in request.args:
            date    = request.args['date']
            if 'offset' in request.args:
                offset = request.args['offset']
                post    = es.search(index='online-news-isa-*', body={
                    "sort": [
                        {"created_at": {"order": "desc"}}
                    ],
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match": {
                                        "source.keyword": "{}".format(str(media_name).lower())
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
                    "from" : offset, "size" : 20
                    })
            else:
                post = es.search(index='online-news-isa-*', body={
                    "sort": [
                        {"created_at": {"order": "desc"}}
                    ],
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match": {
                                        "source.keyword": "{}".format(str(media_name).lower())
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
                    "from": 0, "size": 20
                })
                offset = bool(False)
            total_post  = post['hits']['total']
            jsons['post'] = {'start_offset':int(offset),'total_post': total_post, 'date': date}
            if int(total_post) < 20: loop = total_post
            else: loop = 20
            postingan = []
            for a in range(loop):
                _source = post['hits']['hits'][a]['_source']
                news_id = _source['id']
                pub     = _source['created_at']
                utc_to_local_post = datetime.strptime(pub,'%Y-%m-%d %H:%M:%S')
                utc_to_local_post = utc_to_local_post.replace(tzinfo=from_zone)
                pubdate = str(utc_to_local_post.astimezone(to_zone))
                title   = _source['title']
                url     = _source['link']
                image   = _source['images'][0]
                content = _source['content']

                postingan.append({
                    'content':content,
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

        # return jsons

@app.route('/isa/facebook')
def isa_facebook():
    ip  = parser.get('elastic','isa')
    es  = Elasticsearch(ip, port=5245)
    if 'user_id' in request.args:
        userid  = request.args['user_id']
        query = es.search(index='new-data-facebook-isa-*',body = {
            "sort": [
                {"created_at": {"order": "desc"}}
            ],
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "user_id": "{}".format(userid)
                            }
                        }
                    ]
                }
            },
            "size": 1
        })
        # print query['hits']['hits'][0]['_source']
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
            utc_to_local = datetime.strptime(lasted, '%a %b %d %H:%M:%S %Y')
            utc_to_local = utc_to_local.replace(tzinfo=from_zone)
            last_update = str(utc_to_local.astimezone(to_zone))
        jsons = {"source": source, "last_update": last_update, "last_data_stream": last_data_stream}
        if 'date' in request.args:
            date    = request.args['date']
            spliter = str(date).split('-')
            if spliter.__len__() == 2:
                day = getdays(int(spliter[0]),int(spliter[1]))
                getpost = es.search(index='new-data-facebook-isa-*', body={
                    "sort": [
                        {"created_at": {"order": "desc"}}
                    ],
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match": {
                                        "user_id": userid
                                    }
                                },
                                {
                                    "range": {
                                        "timestamp": {
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
                                "field": "type.keyword"
                            }
                        }
                    },
                    "size": 0
                })
                getcomment = es.search(index='new-data-facebook-isa-*', body={
                    "sort": [
                        {"created_at": {"order": "desc"}}
                    ],
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "query_string": {
                                        "query": "page_id:\"{}\" AND status:\"comment\"".format(userid),
                                        "analyze_wildcard": "true",
                                        "default_field": "*"
                                    }
                                },
                                {
                                    "range": {
                                        "timestamp": {
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
                getpost = es.search(index='new-data-facebook-isa-*', body= {
                "sort": [
                    {"created_at": {"order": "desc"}}
                ],
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "user_id": userid
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
                getcomment = es.search(index='new-data-facebook-isa-*', body={
                "sort": [
                    {"created_at": {"order": "desc"}}
                ],
                "query": {
                    "bool": {
                        "must": [
                            {
                                "query_string": {
                                    "query": "page_id:\"{}\" AND status:\"comment\"".format(userid),
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
                "size": 0
            })
            else:
                getcomment = False
                getpost = False
            totalcom    = getcomment['hits']['total']
            totalpost  = getpost['hits']['total']
            postdetails = getdetails(getpost)
            jsons['Data'] = {'post_total' : totalpost, 'comment' : totalcom}
            if 'details' in request.args: jsons['Data']['post_details'] = postdetails
        result = json.dumps(jsons)
        return result
    else:
        return 'No parameter'

@app.route('/isa/twitter')
def isa_twitter():
    ip  = parser.get('elastic', 'isa')
    es  = Elasticsearch(ip, port=5245)
    if 'username' in request.args:
        username    = request.args['username']
        query   = es.search(index='new-data-twitter-isa-*',body= {
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
        if query['hits']['total'] == 0 :
            last_update = bool(False)
            source = username
            last_data_stream = bool(False)
        else:
            _source = query['hits']['hits'][0]['_source']
            last    = _source['created_at']
            lasted = re.sub('\+[^\s]+', '', last)
            source = _source['username']
            last_data_stream = query['hits']['hits'][0]['_id']
            utc_to_local = datetime.strptime(lasted, '%a %b %d %H:%M:%S %Y')
            utc_to_local    = utc_to_local.replace(tzinfo=from_zone)
            last_update = str(utc_to_local.astimezone(to_zone))
        jsons = {"source" : source, "last_update": last_update, "last_data_stream" : last_data_stream}
        if 'date' in request.args:
            date    = request.args['date']
            spliter = str(date).split('-')
            if spliter.__len__() == 2:
                day = getdays(int(spliter[0]),int(spliter[1]))
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
                                "field": "type.keyword",
                                "size": 4
                            }
                        }
                    },
                    "size": 0
                })
                totaltweet = posttweet['hits']['total']
                totalretweet = postretweet['hits']['total']
                tweetdetails = getdetails(posttweet)
                retweetdetails = getdetails(postretweet)
            elif spliter.__len__() == 3:
                posttweet   = es.search(index='new-data-twitter-isa-*', body = {
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
                postretweet = es.search(index='new-data-twitter-isa-*', body = {
                    "query":
                        {
                            "bool":
                                {
                                    "must":
                                        [
                                            {
                                                "query_string": {
                                                    "query": "retweeted_user_screen_name:\"{0}\" OR mention : \"{0}\" OR in_quote_to_screen_name:\"{0}\"".format(username),
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
                totaltweet  = posttweet['hits']['total']
                totalretweet  = postretweet['hits']['total']
                tweetdetails = getdetails(posttweet)
                retweetdetails = getdetails(postretweet)
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

@app.route('/ipd2/twitter')
def ipd2_twitter():
    ip  = parser.get('elastic', 'ipd2')
    es  = Elasticsearch(ip, port=5200)
    if 'username' in request.args:
        username    = request.args['username']
        query   = es.search(index='ipd-twitter-post-*',body= {
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
            utc_to_local    = utc_to_local.replace(tzinfo=from_zone)
            last_update = str(utc_to_local.astimezone(to_zone))
        jsons = {"source" : source, "last_update": last_update, "last_data_stream" : last_data_stream}
        if 'date' in request.args:
            date    = request.args['date']
            getpost = es.search(index='ipd-criteria-twitter-post-*', body={
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
            posttweet   = es.search(index='ipd-criteria-twitter-post-*', body={
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
                                            "match": {
                                                "type" : "tweet"
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
            postretweet = es.search(index='ipd-criteria-twitter-post-*', body= {
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
                                            "match": {
                                                "type": "retweet"
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
            postreply   = es.search(index='ipd-criteria-twitter-post-*', body={
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
                                            "match": {
                                                "type": "reply"
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
            postquoted = es.search(index='ipd-criteria-twitter-post-*', body={
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
                                            "match": {
                                                "type": "quoted"
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

            totalpost   = getpost['hits']['total']
            totaltweet  = posttweet['hits']['total']
            totalretweet  = postretweet['hits']['total']
            totalreply = postreply['hits']['total']
            totalquoted = postquoted['hits']['total']

            jsons['post'] = {'total_post' : totalpost}
            jsons['post']['details'] = {
                "tweet" : totaltweet,
                "retweet": totalretweet,
                "reply": totalreply,
                "quoted": totalquoted
            }
        if 'monthly' in request.args:
            month = request.args['monthly']
            num = str(month).split('-')
            days = getdays(int(num[0]),int(num[1]))
            getdetails = es.search(index='ipd-twitter-post-*', body={
  "query":{
    "bool":{
      "must":[
        {
          "match": {
            "username": "{}".format(username)

          }

        },
        {
          "range": {
            "created_at": {
              "gte": "{0}-{1}-01 00:00:00".format(int(num[0]),int(num[1])),
              "lte": "{0}-{1}-{2} 23:59:59".format(int(num[0]),int(num[1]),days),
              "format": "yyyy-MM-dd HH:mm:ss",
              "time_zone": "+07:00"

            }

          }

        }]

    }

  },
  "aggs": {
    "type": {
      "terms": {
        "field": "type",
        "size": 4
      }
    }
  },
  "size": 0
})
            totalpost = getdetails['hits']['total']
            details = getdetails['aggregations']['type']['buckets']
            jsons['post'] = {'total_post': totalpost}
            detailsdict = {}
            for a in range(details.__len__()):
                key = details[a]['key']
                val = details[a]['doc_count']
                detailsdict[key] = val
            jsons['post']['details'] = detailsdict
        result = json.dumps(jsons)
        return result
    else:
        return 'No parameter'

@app.route('/isa/onlinenews/news_req')
def isa_onlinenews_news_req():
    ip  = parser.get('elastic','isa')
    es  = Elasticsearch(ip, port=5245)
    if 'news_id' in request.args:
        news_id = request.args['news_id']
        query = es.search(index='online-news-isa-*', body={
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
                                    "id": news_id
                                }
                            }
                        ]
                    }
                },
                "size": 1
        })
        if query['hits']['total'] != 0:
            _source = query['hits']['hits'][0]['_source']
            source = _source['source']
            link = _source['link']
            title = _source['title']
            last = _source['created_at']
            utc_to_local = datetime.strptime(last, '%Y-%m-%d %H:%M:%S')
            utc_to_local = utc_to_local.replace(tzinfo=from_zone)
            last_update = str(utc_to_local.astimezone(to_zone))
            image = _source['images'][0]
            content = _source['content']
        else:
            last_update = bool(False)
            link = bool(False)
            title = bool(False)
            source = news_id
            image = bool(False)
            content = bool(False)
        jsons = {"source": source, "post" : {'news_id':news_id,"title": title,"pubdate": last_update, "link": link,'content':content,'image' : image }}
        result = json.dumps(jsons)
        return result
    else:
        return 'No parameters'


if __name__ == '__main__':
    # app.run(host='192.168.20.92',port=5002)
    app.run(port=5002)