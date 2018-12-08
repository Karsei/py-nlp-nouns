#!/usr/bin/env python
#-*- coding: utf-8 -*-

"""
- 데이터 전처리기 (Data Preprocessing)

Author By. Karsei

First Written Date. 2016/12/30
Last Modified Date. 2016/12/30
"""

# 문자 인코딩 관련
import sys
reload(sys)
sys.setdefaultencoding("utf8")

# json 포멧 관련
import json

# 정규화
import re

# 형태소 분석
from konlpy.tag import Twitter
postag = Twitter()

# mongodb 접속 관련
import pymongo

# 예쁘게 출력
import pprint

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
* 데이터 전처리
 - 1. 영어 소문자화
 - 2. 정규화로 불필요한 문자 삭제
 - 3. 명사 추출
 - 4. 불용어 제거
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
class SentenceProcessing:
    # 정규화 목록
    pre_regex_list = [
        r'<[^>]+>',         # HTML 태그
        r'(?:@[\w_]+)',     # @ 트윗 태그
        r'(http|https):\/\/([\xA1-\xFEa-z0-9_\-]+\.[\xA1-\xFEa-z0-9:;&#@=_~%\?\/\.\,\+\-]+)',   # URL 주소
        #r"(?:([a-zㄱ-ㅎㅏ-ㅣ가-힣0-9]+[\"'\-_]+[a-zㄱ-ㅎㅏ-ㅣ가-힣0-9]+)|([\"'\-_]+[a-zㄱ-ㅎㅏ-ㅣ가-힣0-9]+))",    # 하이픈과 언더바, 작은 따옴표로 연결된 단어
        #r"(?:[a-zㄱ-ㅎㅏ-ㅣ가-힣0-9]+[\"'\-_]+[a-zㄱ-ㅎㅏ-ㅣ가-힣0-9]+)",   # 하이픈과 언더바로 연결된 단어
        r'(?:\#([0-9a-zA-Z가-힣]*))',    # 해시 태그
        #r"(?:[\"'\-_,.]+)"  # 문장 부호
        r'(?:[\"\'＂＇“”‘’\-_,!@#$%^&*()\[\].、,…]+)'  # 문장 부호
        #r'(?:[a-zㄱ-ㅎㅏ-ㅣ가-힣0-9_]+)' # 나머지
    ]
    # 정규화 컴파일
    pre_regex_token = None
    # 불용어 파일
    pre_stopwords_file = None

    """""""""""""""""""""""""""""""""""""""""""""""
    # 생성자
    """""""""""""""""""""""""""""""""""""""""""""""
    def __init__(self):
        # 계속 사용할 정규화를 컴파일
        #self.pre_regex_token = re.compile(unicode(r'('+'|'.join(self.pre_regex_list)+')', 'utf-8'), re.UNICODE)
        self.pre_regex_token = re.compile(r'('+'|'.join(self.pre_regex_list)+')', re.UNICODE)
        # 불용어 파일 로드
        with open('ko_stopwords.json') as pre_json_file:
            self.pre_stopwords_file = json.load(pre_json_file)

    """""""""""""""""""""""""""""""""""""""""""""""
    # 문자열 정규화
    @return 문자열
    """""""""""""""""""""""""""""""""""""""""""""""
    def getRegexString(self, msg):
        return re.sub(self.pre_regex_token, "", msg)

    """""""""""""""""""""""""""""""""""""""""""""""
    # 문자열에서 명사 추출
    @return 리스트
    """""""""""""""""""""""""""""""""""""""""""""""
    def getNounList(self, msg):
        return postag.nouns(msg)

    """""""""""""""""""""""""""""""""""""""""""""""
    # 불용어 제거
    @return 리스트
    """""""""""""""""""""""""""""""""""""""""""""""
    def removeStopwords(self, list):
        for nouns in list:
            for stopwords in self.pre_stopwords_file:
                if nouns == stopwords:
                    if nouns in list:
                        list.remove(nouns)
        return list

    """""""""""""""""""""""""""""""""""""""""""""""
    # 리스트 내부 문자 출력
    """""""""""""""""""""""""""""""""""""""""""""""
    def printList(self, listset):
        # 이 작업을 안하면 콘솔에 유니코드 포멧이 그대로 찍혀서 나타난다.
        print repr([str(x).encode(sys.stdout.encoding) for x in listset]).decode('string-escape')

    """""""""""""""""""""""""""""""""""""""""""""""
    # 딕셔너리 내부 문자 출력
    """""""""""""""""""""""""""""""""""""""""""""""
    def printDict(self, Dictset):
        # 이 작업을 안하면 콘솔에 유니코드 포멧이 그대로 찍혀서 나타난다.
        print repr({str(x).encode(sys.stdout.encoding) for x in Dictset}).decode('string-escape')

    """""""""""""""""""""""""""""""""""""""""""""""
    # 데이터 전처리 수행
    @return 리스트
    """""""""""""""""""""""""""""""""""""""""""""""
    def dataPreprocess(self, msg):
        tempset = self.getRegexString(msg.lower())
        print tempset
        return self.removeStopwords(self.getNounList(tempset))

# 시스템 인자값 받기
if len(sys.argv) < 2:
    print "Usage: python %s dbName" % sys.argv[0]
    sys.exit()
sys_dbName = sys.argv[1]

# 전처리기 사용
p = SentenceProcessing()

# DB 접속
db_connection = pymongo.MongoClient("localhost", 65000)
db_connection['twitterset'].authenticate('twitbot', 'twitbotpass', mechanism='SCRAM-SHA-1')
db_database = db_connection['twitterset']
db_collection = db_database[sys_dbName]
db_collection_result = db_database[sys_dbName + '_wordtotal']

# 데이터 로드
docs = db_collection.find()        # DB에서 모든 데이터 로드
for record in docs:
    for words in p.dataPreprocess(record['text']):
        db_collection_result.update(
            {'word': words},
            {'$inc': {'count': 1}},
            upsert = True
        )

    p.printList(p.dataPreprocess(record['text']))

# 빈도 결과 확인
count = 0
docs_result = db_collection_result.find().sort('count', -1)
for wordset in docs_result:
    count = count + 1
    pprint.pprint(wordset)
    if count == 20:
        break
print "Total Word count: " + str(db_collection_result.count())

db_connection.close()

#sentence = u"대체 저들이 박근혜등과 무슨 차이가 있다는 말인가. 하나같은 기득권들이다. 똑같은 것들이 뭐가 잘났다고 서로 못잡아 먹어 안달인가.다를 바 없다.호남 정치인도 다 똑같다.그러니 호남만 욕하지 마라."
#sentence1 = u"아버지가방에들어가신다"
#getnoun = p.dataPreprocess(sentence)
#p.printList(getnoun)
