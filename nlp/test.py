#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding("utf8")

import re

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
strset = "자동차용어 #품앗이 조마조마 #맞팔 피터팬 인스타 #쌘놈 WM엔터테인먼트 김유정 #선팔 #메가팔로우 #해시태그 유연석 #sns #페이스북 #팔로워 #사람찾기 구글 씨엔블루팬덤 #팔로잉 포토 #megafollow #좋아요 맥북 #인스타그램 사이버"
#pre_regex_token = re.compile(unicode(r'('+'|'.join(pre_regex_list)+')', 'utf-8'), re.UNICODE)
pre_regex_token = re.compile(r'('+'|'.join(pre_regex_list)+')', re.UNICODE)
print re.sub(pre_regex_token, "", strset)
#token_regex = re.compile(r'('+'|'.join(regex_str)+')')
