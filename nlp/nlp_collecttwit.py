#!/usr/bin/env python
#-*- coding: utf-8 -*-

"""
- 트위터 트윗 수집기 (Collect Twitter Twits)

Author By. Karsei

First Written Date. 2016/12/29
Last Modified Date. 2017/01/01
"""

# 문자 인코딩과 시스템 입력 관련
import sys
reload(sys)
sys.setdefaultencoding("utf8")

# json 포멧 관련
import json

# mongodb 접속 관련
import pymongo

# 트윗 과부하 방지를 위한 예외 처리
#from http.client import IncompleteRead # Python 2
#from httplib import IncompleteRead #Python 3
from requests.packages.urllib3.exceptions import ProtocolError

# 수집 시 날짜 출력
from datetime import datetime

# 트위터 스트리밍
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# 트위터 API를 이용하기 위한 Key
CONSUMER_KEY = '____CONSUMER_KEY____'
CONSUMER_SECRET = '____CONSUMER_SECRET____'
OAUTH_ACCESS_TOKEN = '____OAUTH_ACCESS_TOKEN____'
OAUTH_ACCESS_SECRET = '____OAUTH_ACCESS_SECRET____'

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
* 로그 생성
 - 로그 메세지 형성
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
class LogWriter():
    """""""""""""""""""""""""""""""""""""""""""""""
    # 현재 시간 문자열 출력
    """""""""""""""""""""""""""""""""""""""""""""""
    def getCurrentDate(self):
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    """""""""""""""""""""""""""""""""""""""""""""""
    # 정보
    # ex) log.i("{0} yay {1}!", ("so", "1"))
    """""""""""""""""""""""""""""""""""""""""""""""
    def i(self, msg, formats=(), dateset=False):
        self.custom("정보", msg, formats, dateset)

    """""""""""""""""""""""""""""""""""""""""""""""
    # 오류
    # ex) log.e("{0} yay {1}!", ("so", "1"))
    """""""""""""""""""""""""""""""""""""""""""""""
    def e(self, msg, formats=(), dateset=False):
        self.custom("오류", msg, formats, dateset)

    """""""""""""""""""""""""""""""""""""""""""""""
    # 경고
    # ex) log.w("{0} yay {1}!", ("so", "1"))
    """""""""""""""""""""""""""""""""""""""""""""""
    def w(self, msg, formats=(), dateset=False):
        self.custom("경고", msg, formats, dateset)

    """""""""""""""""""""""""""""""""""""""""""""""
    # 수동
    # ex) log.custom("LOG", "{0} yay {1}!", ("so", "1"))
    """""""""""""""""""""""""""""""""""""""""""""""
    def custom(self, title, msg, formats=(), dateset=False):
        if len(formats) > 0:
            #print "[" + title + "] " + msg.format(*formats)
            if dateset == True:
                print "[" + title + "][" + self.getCurrentDate() + "] " + msg.format(formats)
            else:
                print "[" + title + "] " + msg.format(formats)
        else:
            if dateset == True:
                print "[" + title + "][" + self.getCurrentDate() + "] " + msg
            else:
                print "[" + title + "] " + msg

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
* 통계 로그
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
def showResult(start_date, search_word, total_count, retry_count):
    # 시간 계산
    end_date = datetime.now()
    totaltime = end_date - start_date

    # 출력
    print " "
    log.custom("통계", "- 검색 단어 :: {0:s}", search_word)
    log.custom("통계", "- 메세지 확인 갯수 :: {0:s}개", str(total_count))
    log.custom("통계", "- 재시도 횟수 :: {0:s}개", str(retry_count))
    log.custom("통계", "- 시작 시간 :: {0:s}", start_date.strftime('%Y-%m-%d %H:%M:%S'))
    log.custom("통계", "- 종료 시간 :: {0:s}", end_date.strftime('%Y-%m-%d %H:%M:%S'))
    log.custom("통계", "- 걸린 시간 :: %s일 %.2d시간 %.2d분 %.2d초" % (totaltime.days, totaltime.seconds // 3600, (totaltime.seconds // 60) % 60, totaltime.seconds % 60))

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
* 데이터 수집
 - 트위터 API를 이용한 스트리밍 데이터 획득
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
class TwitterStreamer(StreamListener):
    # 수집 단어
    word = None
    # 수집된 갯수 파악
    count = 0

    """""""""""""""""""""""""""""""""""""""""""""""
    # 수집 단어 얻기
    """""""""""""""""""""""""""""""""""""""""""""""
    def getSearchWord(self):
        return self.word

    """""""""""""""""""""""""""""""""""""""""""""""
    # 수집 단어 설정
    """""""""""""""""""""""""""""""""""""""""""""""
    def setSearchWord(self, wordset):
        self.word = wordset

    """""""""""""""""""""""""""""""""""""""""""""""
    # 수집된 갯수 얻기
    """""""""""""""""""""""""""""""""""""""""""""""
    def getRecvCount(self):
        return self.count

    """""""""""""""""""""""""""""""""""""""""""""""
    # 수집된 갯수 설정
    """""""""""""""""""""""""""""""""""""""""""""""
    def setRecvCount(self, amount):
        self.count = amount

    """""""""""""""""""""""""""""""""""""""""""""""
    # 간단한 정보를 위한 메서드
    """""""""""""""""""""""""""""""""""""""""""""""
    #def on_status(self, status):
    #    self.count += 1
    #    print str(self.count) + " - " + status.text

    """""""""""""""""""""""""""""""""""""""""""""""
    # 자세한 정보를 위한 메서드
    """""""""""""""""""""""""""""""""""""""""""""""
    def on_data(self, data):
        try:
            # 데이터가 JSON이므로 JSON 형식으로 로드
            stm_json_load = json.loads(data)
        except Exception, msg:
            print "[오류] 스트리밍 수집 오류: " + str(msg)
            return False

        # 트윗 갯수 증가
        self.count += 1

        # 데이터 정리
        stm_dict_dataset = { }
        if 'retweeted_status' in stm_json_load:
            stm_dict_dataset["id_str"] = stm_json_load['retweeted_status']['id_str']
            stm_dict_dataset["timestamp"] = stm_json_load['retweeted_status']['created_at']
            stm_dict_dataset["retweet"] = 1
            stm_dict_dataset["retcount"] = stm_json_load['retweeted_status']['retweet_count']
            stm_dict_dataset["text"] = stm_json_load['retweeted_status']['text']
        else:
            stm_dict_dataset["id_str"] = stm_json_load['id_str']
            stm_dict_dataset["timestamp"] = stm_json_load['created_at']
            stm_dict_dataset["retweet"] = 0
            stm_dict_dataset["retcount"] = stm_json_load['retweet_count']
            stm_dict_dataset["text"] = stm_json_load['text']

        # 중복 값을 제거하기 위해 upsert 사용
        #db_collection.insert(stm_dict_dataset)
        db_collection.update(
            {'id_str': stm_dict_dataset["id_str"]},
                {'$set':
                    {'timestamp': stm_dict_dataset["timestamp"],
                    'retweet': stm_dict_dataset["retweet"],
                    'retcount': stm_dict_dataset["retcount"],
                    'text': stm_dict_dataset["text"]
                }
            },
            upsert = True
        )

        print "[수집중][%s]" % (datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + stm_dict_dataset["text"]
        print "[수집중][%s] 지금까지 " % (datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + str(self.count) + "개의 트윗 확인 (" + self.getSearchWord() + ")"

        return True

    """""""""""""""""""""""""""""""""""""""""""""""
    # 오류 메서드
    """""""""""""""""""""""""""""""""""""""""""""""
    def on_error(self, status):
        #Ref. https://dev.twitter.com/overview/api/response-codes
        print "[오류] 스트리밍 오류가 발생했습니다! (코드 " + str(status) + ")"
        if status == 420:
            #Ref. https://dev.twitter.com/rest/public/rate-limiting
            print "[오류] 오류 코드 %s - Enhance Your Calm :: Rate 제한에 걸렸기 때문에 메세지를 받을 수 없습니다." % str(status)
        elif status == 500:
            print "[오류] 오류 코드 %s - Internal Server Error :: 흠, 뭔가 내부적으로 잘못된 것 같습니다!" % str(status)
        return True

# 시스템 인자값 받기
if len(sys.argv) < 3:
    print "Usage: python %s SearchWord CollectionName" % sys.argv[0]
    sys.exit()
sys_searchWord = sys.argv[1]
sys_dbName = sys.argv[2]

"""
# SQL 데이터베이스 접속 (없을 시에는 생성)
"""
db_connection = pymongo.MongoClient("localhost", 65000)
db_connection['twitterset'].authenticate('twitbot', 'twitbotpass', mechanism='SCRAM-SHA-1')
db_database = db_connection['twitterset']
db_collection = db_database[sys_dbName + datetime.now().strftime('_%Y%m%d')]
db_collection.create_index([("id_str", pymongo.DESCENDING)], unique=True)

start_date = datetime.now()
retry_count = 0
end_date = None

# 트위터 API 접근
twitter_auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
twitter_auth.set_access_token(OAUTH_ACCESS_TOKEN, OAUTH_ACCESS_SECRET)
twitter_api = tweepy.API(twitter_auth)

# 트위터 스트리밍 접근
#search_word = u'새해'
search_word = sys_searchWord
total_count = 0
retry_count = 0

# 로그 작성
log = LogWriter()

print "-------------- # 수집을 시작합니다 #--------------"

while True:
    try:
        twitter_stream_lis = TwitterStreamer()

        # 수집 단어 설정
        twitter_stream_lis.setSearchWord(search_word)

        # 이전에 얻었던 메세지 갯수 갱신
        twitter_stream_lis.setRecvCount(total_count)

        # 스트리밍 설정
        twitter_stream = Stream(auth=twitter_auth, listener=twitter_stream_lis)

        # 스트리밍 필터 설정
        twitter_stream.filter(track=[search_word])
    except KeyError, e:
        ## 키 오류로 중단되면 다시 연결한 후 수집 이어서 하기
        # 재시도 횟수 증가
        retry_count += 1

        #이전까지 획득한 메세지 갯수 갱신
        total_count = twitter_stream_lis.getRecvCount()

        log.e("KeyError - 다시 시도... (횟수 " + str(retry_count) + "번)")
        log.e("이유: " + str(e))
        continue
    except ProtocolError, e:
        ## 끊어지면 다시 연결한 후 수집 이어서 하기
        # 재시도 횟수 증가
        retry_count += 1

        #이전까지 획득한 메세지 갯수 갱신
        total_count = twitter_stream_lis.getRecvCount()

        log.e("IncompleteRead - 다시 시도... (횟수 " + str(retry_count) + "번)")
        continue
    except KeyboardInterrupt:
        # 스트리밍 연결 종료
        twitter_stream.disconnect()

        #이전까지 획득한 메세지 갯수 갱신
        total_count = twitter_stream_lis.getRecvCount()

        # 통계 출력
        showResult(start_date, search_word, total_count, retry_count)
        #log.custom("통계", "- 검색 단어 :: {0:s}", search_word)
        #log.custom("통계", "- 메세지 확인 갯수 :: {0:s}개", str(total_count))
        #log.custom("통계", "- 재시도 횟수 :: {0:s}개", str(retry_count))
        #log.custom("통계", "- 시작 시간 :: {0:s}", start_date.strftime('%Y-%m-%d %H:%M:%S'))
        #log.custom("통계", "- 종료 시간 :: {0:s}", end_date.strftime('%Y-%m-%d %H:%M:%S'))
        #log.custom("통계", "- 걸린 시간 :: %s일 %.2d시간 %.2d분 %.2d초" % (totaltime.days, totaltime.seconds // 3600, (totaltime.seconds // 60) % 60, totaltime.seconds % 60))
        print "-------------- # 수집을 종료합니다 #--------------"
        break
    except Exception, msg:
        log.e("예상하지 못한 오류가 발생했습니다.")
        log.e("이유: " + str(msg))

        ## 끊어지면 다시 연결한 후 수집 이어서 하기
        # 재시도 횟수 증가
        retry_count += 1

        #이전까지 획득한 메세지 갯수 갱신
        total_count = twitter_stream_lis.getRecvCount()

        log.e("Exception - 다시 시도... (횟수 " + str(retry_count) + "번)")

        # 통계 출력
        #showResult(start_date, search_word, total_count, retry_count)
        #print "-------------- # 수집을 종료합니다 #--------------"
        continue

# 데이터베이스 연결 종료
db_connection.close()
