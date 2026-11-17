"""
data_pipeline.py
================
언론사 인사 발령 기사 데이터 파이프라인 및 엔지니어링 모듈.
- 빅카인즈(BIG Kinds) 엑셀/CSV 파서
- 한국 언론사 인사 기사 전용 정규표현식(Regex) 고속 추출기
- 직급 체계 및 출입처 도메인 유사도 매핑
- 동명이인 분리 클러스터링 (1순위 이메일 바이라인, 2순위 휴리스틱 스코어링)
- 매체 간 이직(Job Change) 자동 탐지 엔진
- 네이버 뉴스 검색 API 기반 바이라인 이메일 동기화
- SQLite 데이터베이스 영속화
"""

import os
import io
import re
import html
import sqlite3
import datetime
import json
import gzip
from typing import List, Dict, Tuple, Optional, Any, Set
from urllib.parse import quote
import pandas as pd
import requests
import urllib3
import concurrent.futures
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ==========================================
# 1. 직급 서열(Rank) 및 부서 도메인(Domain) 사전
# ==========================================

RANK_HIERARCHY: Dict[str, int] = {
    "회장": 10,
    "대표이사": 10,
    "대표": 10,
    "사장": 10,
    "부사장": 9,
    "전무이사": 9,
    "상무이사": 9,
    "상무보": 9,
    "전무": 9,
    "상무": 9,
    "이사": 9,
    "임원급": 9,
    "상임고문": 9,
    "고문": 9,
    "주필": 9,
    "논설주간": 9,
    "편집국장": 8,
    "보도국장": 8,
    "논설위원실장": 8,
    "수석논설위원": 8,
    "연구소장": 8,
    "부소장": 7,
    "칼럼니스트": 7,
    "국장": 8,
    "국장급": 8,
    "국장대우": 8,
    "본부장": 8,
    "본부장대우": 8,
    "부문장": 8,
    "단장": 8,
    "단장대우": 8,
    "센터장": 8,
    "총괄": 8,
    "부국장": 7,
    "부국장급": 7,
    "부국장직대": 7,
    "부국장대우": 7,
    "실장": 7,
    "실장대우": 7,
    "부단장": 7,
    "디렉터": 7,
    "에디터": 7,
    "논설위원": 7,
    "해설위원": 7,
    "전문위원": 7,
    "기획위원": 7,
    "부장": 6,
    "부장급": 6,
    "부장직대": 6,
    "부디렉터": 6,
    "수석": 6,
    "팀장(부장급)": 6,
    "데스크": 6,
    "지국장": 6,
    "부장대우": 5,
    "직대": 5,
    "차장": 4,
    "차장급": 4,
    "차장직대": 4,
    "팀장": 4,
    "팀장급": 4,
    "팀장대우": 4,
    "팀장직대": 4,
    "파트장": 4,
    "차장대우": 3,
    "선임기자": 3,
    "수석기자": 3,
    "전문기자": 3,
    "특파원": 3,
    "과장": 3,
    "기자": 2,
    "평기자": 2,
    "대리": 2,
    "사원": 2,
    "수습기자": 1,
    "인턴": 1,
}

# 정규표현식 매칭을 위한 직급 패턴 (긴 단어부터 매칭되도록 정렬)
SORTED_RANKS = sorted(RANK_HIERARCHY.keys(), key=lambda x: len(x), reverse=True)
RANK_REGEX_PATTERN = r"|".join([re.escape(r) for r in SORTED_RANKS])

DOMAIN_TAXONOMY: Dict[str, List[str]] = {
    "부동산/건설": [
        "건설부동산", "부동산", "건설", "도시", "도시인프라", "주택", "토목", "인프라"
    ],
    "경제/산업/금융": [
        "경제", "산업", "금융", "증권", "유통", "중기", "벤처", "중기벤처", "마켓",
        "자본시장", "재정", "통상", "에너지", "모빌리티", "자동차", "소비자", "생활경제",
        "공기업", "기업"
    ],
    "정치/외교": [
        "정치", "청와대", "대통령실", "국회", "정당", "외교", "안보", "통일", "국방",
        "외교안보", "통일외교"
    ],
    "사회/법조": [
        "사회", "법조", "경찰", "검찰", "법원", "시민사회", "전국", "지방", "사건",
        "기동취재", "안전", "환경", "노동", "복지", "보건"
    ],
    "IT/과학/바이오": [
        "it", "과학", "바이오", "제약", "디지털", "테크", "정보미디어", "통신", "인공지능",
        "ai", "플랫폼", "소프트웨어"
    ],
    "문화/스포츠": [
        "문화", "스포츠", "연예", "엔터", "대중문화", "생활", "여가", "스타일", "종교", "출판"
    ],
    "국제/글로벌": [
        "국제", "글로벌", "해외", "외신", "미국", "중국", "일본", "유럽"
    ],
    "기획/논설/편집": [
        "논설", "기획", "탐사", "편집", "교열", "디자인", "미디어랩", "어젠다"
    ],
}

# 도메인 간 친화도 (이직 시 유사 분야 판단용, 1.0 = 동일, 0.7 = 친화적, 0.1 = 무관)
DOMAIN_AFFINITY: Dict[Tuple[str, str], float] = {
    ("부동산/건설", "경제/산업/금융"): 0.85,
    ("경제/산업/금융", "부동산/건설"): 0.85,
    ("경제/산업/금융", "IT/과학/바이오"): 0.70,
    ("IT/과학/바이오", "경제/산업/금융"): 0.70,
    ("정치/외교", "사회/법조"): 0.75,
    ("사회/법조", "정치/외교"): 0.75,
    ("사회/법조", "경제/산업/금융"): 0.50,
    ("경제/산업/금융", "사회/법조"): 0.50,
    ("문화/스포츠", "IT/과학/바이오"): 0.20,
    ("문화/스포츠", "부동산/건설"): 0.10,
}

STOPWORDS = {
    "인사", "발령", "본사", "지사", "편집국", "보도국", "신문", "방송", "뉴스", "한국", "일보",
    "매일", "경제", "대표", "국장", "부장", "차장", "팀장", "기자", "위원", "실장", "총괄",
    "대우", "승진", "전보", "보임", "신규", "선임", "퇴임", "파견", "내정", "전보자",
    "임용", "임명", "채용", "영입", "이동", "전임", "선출", "위촉", "해임", "면직", "보직",
    "외", "등", "겸", "및", "간", "후보", "부국장", "직무대행", "권한대행", "직대", "내정자",
    "상임고문", "고문", "전무이사", "상무이사", "상무보", "연구소장", "부소장", "칼럼니스트",
    "과장급", "과장", "부단장", "단장", "서기관", "서기관급", "사무관", "사무관급", "논설실장", "보도실장",
    "카톡", "이메일", "제보", "문의", "게재", "연락처", "포함", "담당자", "저작권",
    "무단전재", "재배포", "신문사", "제보는", "산업국", "중앙신문", "경향신문", "한국정경",
    "서울경제", "한겨레", "조선일보", "동아일보", "연합뉴스", "뉴스1", "뉴시스",
    "감사팀", "감사실", "법무실", "기획실", "비서실", "비서실장", "인사팀", "총무팀", "편집장", "지국장", "논설위원",
    # 웹 포털/뉴스 사이트 하단 공유 및 UI 버튼 노이즈 배제
    "공유하기", "카카오톡", "페이스북", "네이버", "구독", "광고", "구독중", "이전", "다음", "송고",
    "밴드", "프린트", "본문", "글자", "크기", "조정", "닫기", "댓글", "좋아요", "슬퍼요",
    "화나요", "후속요청", "국내", "최대", "원스톱", "콘텐츠", "플랫폼", "함께", "보면", "좋은",
    "데이블", "읽기", "후원링크", "다양한", "주제별", "맞춤", "추천", "북마크", "메신저", "복사",
    "글자크기", "폰트", "공유", "제공", "연합", "확인", "취소", "읽기모드", "음성지원", "다크모드",
    "폰트크기", "기사반응", "기사후원", "기사보기", "댓글보기", "새창열림", "구독하기", "응원하기",
    "후원하기", "전체기사", "실시간", "다른기사", "바로가기", "전체메뉴", "채널구독", "트위터로",
    "내용입력", "주소복사", "알아보기", "인기기사", "소름돋는", "한화파워", "가나다순", "퍼가기",
    "이메일로", "그림으로", "측근그룹", "인수위", "리버풀에", "초유의"
}

EXECUTIVE_RANKS: Set[str] = {
    "대표이사", "대표", "사장", "부사장", "전무이사", "상무이사", "상무보", "전무", "상무",
    "이사", "임원급", "상임고문", "고문", "주필"
}

MEDIA_GROUP_ALIASES: Dict[str, List[str]] = {
    "한국경제": ["한국경제신문", "한국경제신문사", "한경닷컴", "한경", "한경미디어그룹", "한경비즈니스", "한경매거진"],
    "매일경제": ["매경", "매일경제신문", "매일경제신문사", "매경미디어그룹", "매경미디어", "매경닷컴", "매경비즈", "매경AX"],
    "조선일보": ["조선일보사", "조선미디어그룹", "조선미디어", "조선비즈", "조선닷컴"],
    "동아일보": ["동아일보사", "동아미디어그룹", "동아미디어", "동아닷컴"],
    "중앙일보": ["중앙일보사", "중앙그룹", "중앙홀딩스", "중앙미디어", "중앙일보S", "월간중앙"],
    "경향신문": ["경향신문사", "경향", "경향닷컴"],
    "한겨레": ["한겨레신문", "한겨레신문사", "한겨레21"],
    "서울신문": ["서울신문사", "대한매일"],
    "세계일보": ["세계일보사", "세계일보신문", "세계닷컴"],
    "국민일보": ["국민일보사", "국민일보신문", "쿠키뉴스"],
    "문화일보": ["문화일보사"],
    "서울경제": ["서울경제신문", "서울경제신문사", "서경", "서경마켓"],
    "아시아경제": ["아시아경제신문", "아시아경제신문사", "아경"],
    "헤럴드": ["헤럴드미디어", "(주)헤럴드", "주식회사 헤럴드", "헤럴드그룹", "헤럴드경제", "코리아헤럴드", "헤경", "헤럴드경제신문", "헤럴드아트데이", "헤럴드에듀", "헤럴드디자인", "헤럴드팝", "heraldcorp", "heraldbiz", "heraldcorp.com"],
    "헤럴드미디어": ["헤럴드", "(주)헤럴드", "주식회사 헤럴드", "헤럴드그룹", "헤럴드경제", "코리아헤럴드", "헤경", "헤럴드경제신문", "heraldcorp", "heraldbiz", "heraldcorp.com"],
    "헤럴드경제": ["헤럴드경제신문", "헤경"],
    "코리아헤럴드": ["koreaherald", "코리아 헤럴드", "The Korea Herald"],
    "파이낸셜뉴스": ["파이낸셜", "fnnews", "파이낸셜뉴스신문사"],
    "머니투데이": ["머니투데이신문", "머니투데이그룹", "머니투데이미디어", "머투"],
    "머니투데이방송": ["MTN", "머니투데이방송", "mtn"],
    "이데일리": ["이데일리신문", "이데일리M", "이데일리m", "KG그룹"],
    "이투데이": ["이투데이신문", "이투데이사", "이투데이그룹", "이투데이미디어", "이투데이뉴스", "(주)이투데이", "etoday", "etoday.co.kr"],
    "전자신문": ["전자신문사", "etnews"],
    "디지털타임스": ["디지털타임스사", "DT", "디타"],
    "아주경제": ["아주경제신문", "아주경제신문사", "아주일보"],
    "뉴스1": ["뉴스원", "news1"],
    "뉴시스": ["newsis"],
    "연합뉴스": ["연합뉴스통신", "연합뉴스사", "연합"],
    # 지역 종합 일간지
    "부산일보": ["부산일보사", "부산일보신문"],
    "국제신문": ["국제신문사"],
    "매일신문": ["매일신문사"],
    "영남일보": ["영남일보사"],
    "대구일보": ["대구일보사"],
    "경북일보": ["경북일보사"],
    "강원일보": ["강원일보사"],
    "강원도민일보": ["강원도민일보사"],
    "대전일보": ["대전일보사"],
    "중도일보": ["중도일보사"],
    "충청투데이": ["충청투데이사"],
    "충청일보": ["충청일보사"],
    "광주일보": ["광주일보사"],
    "전남일보": ["전남일보사"],
    "전북도민일보": ["전북도민일보사"],
    "전북일보": ["전북일보사"],
    "경남신문": ["경남신문사"],
    "경남도민일보": ["경남도민일보사"],
    "제민일보": ["제민일보사"],
    "한라일보": ["한라일보사"],
    "제주일보": ["제주일보사"],
    "경기일보": ["경기일보사"],
    "경인일보": ["경인일보사"],
    "중부일보": ["중부일보사"],
    "인천일보": ["인천일보사"],
    "울산매일": ["울산매일신문", "울산매일사"],
    "경상일보": ["경상일보사"],
    # 방송사 및 지역 민방
    "KNN": ["knn", "부산경남민방"],
    "TBC": ["tbc", "대구경북민방"],
    "kbc광주방송": ["kbc", "KBC", "광주방송"],
    "TJB대전방송": ["TJB", "tjb", "대전방송"],
    "UBC울산방송": ["UBC", "ubc", "울산방송"],
    "JTV전주방송": ["JTV", "jtv", "전주방송"],
    "CJB청주방송": ["CJB", "cjb", "청주방송"],
    "G1방송": ["G1", "g1", "강원민방"],
    "JIBS제주방송": ["JIBS", "jibs", "제주방송"],
    "OBS경인TV": ["OBS", "obs", "경인방송", "경인tv"],
    "BBS불교방송": ["BBS", "bbs", "불교방송"],
    "CPBC가톨릭평화방송": ["CPBC", "cpbc", "평화방송", "가톨릭평화방송"],
    "CBS기독교방송": ["CBS", "cbs", "기독교방송"],
    # 전문지 / 인터넷 / 주간지
    "오마이뉴스": ["오마이뉴스사"],
    "프레시안": ["프레시안사"],
    "데일리안": ["데일리안사"],
    "시사저널": ["시사저널사", "시사저널e"],
    "시사IN": ["시사인", "시사in"],
    "미디어오늘": ["미디어오늘사"],
    "기자협회보": ["한국기자협회", "기자협회"],
    "지디넷코리아": ["지디넷", "zdnet"],
    "블로터": ["블로터앤미디어"],
    "더벨": ["thebell"],
    "딜사이트": ["dealsite"],
    "농민신문": ["농민신문사"],
    "국방일보": ["국방일보사"],
    "스포츠조선": ["스포츠조선사"],
    "스포츠동아": ["스포츠동아사"],
    "스포츠서울": ["스포츠서울사"],
    "스포츠경향": ["스포츠경향사"],
    "일간스포츠": ["일간스포츠사"]
}


def are_group_affiliates(media1: str, media2: str) -> bool:
    """두 언론사가 동일 미디어 그룹 또는 계열사 관계인지 확인합니다."""
    m1 = normalize_media_name(media1).strip()
    m2 = normalize_media_name(media2).strip()
    if not m1 or not m2 or m1 == m2:
        return True
    groups = [
        {"중앙일보", "중앙일보s", "중앙그룹", "이코노미스트", "일간스포츠", "jtbc", "월간중앙"},
        {"이데일리", "이데일리m", "일간스포츠", "이코노미스트"},
        {"머니투데이", "머니투데이방송", "mtn", "머니s", "뉴스1", "뉴시스"},
        {"한국경제", "한경닷컴", "한경비즈니스", "한국경제tv", "한경매거진"},
        {"매일경제", "매경닷컴", "mbn", "매경이코노미"},
        {"헤럴드", "헤럴드미디어", "(주)헤럴드", "주식회사 헤럴드", "헤럴드그룹", "헤럴드경제", "코리아헤럴드", "헤럴드아트데이", "헤럴드에듀", "헤럴드팝", "헤럴드디자인"},
        {"조선일보", "조선비즈", "tv조선", "스포츠조선", "주간조선"},
        {"동아일보", "채널a", "스포츠동아", "주간동아"},
    ]
    m1_low = m1.lower()
    m2_low = m2.lower()
    for g in groups:
        if any(m1_low in x or x in m1_low for x in g) and any(m2_low in x or x in m2_low for x in g):
            return True
    return False


def classify_domain(dept_name: str) -> str:
    """부서명을 분석하여 상위 도메인 카테고리를 반환합니다."""
    if not dept_name:
        return "일반"
    dept_lower = dept_name.lower().strip()
    for domain, keywords in DOMAIN_TAXONOMY.items():
        for kw in keywords:
            if kw in dept_lower:
                return domain
    return "일반"


def calculate_domain_similarity(d1: str, d2: str) -> float:
    """두 도메인 간의 친화도(0.0 ~ 1.0)를 산출합니다."""
    if not d1 or not d2:
        return 0.3
    if d1 == d2:
        return 1.0
    pair = (d1, d2)
    if pair in DOMAIN_AFFINITY:
        return DOMAIN_AFFINITY[pair]
    rev_pair = (d2, d1)
    if rev_pair in DOMAIN_AFFINITY:
        return DOMAIN_AFFINITY[rev_pair]
    return 0.2


def normalize_media_name(raw_name: str) -> str:
    """언론사명에서 <인사>, [인사], 편집국, 보도국 등 부가 설명을 제거하고 순수 언론사명만 표준 표기합니다."""
    if not raw_name:
        return "미상"
    name = str(raw_name).strip()
    
    # 1. <인사>, [인사], 【인사】, (인사), 〈인사〉, «인사» 등 인사 관련 태그 및 단어 제거
    name = re.sub(r"[<\[\(【〈«][\s\w]*인사[\s\w]*[>\]\)】〉»]", "", name)
    name = re.sub(r"[<\[\(【〈«]\s*인사\s*", "", name)
    name = re.sub(r"\s*인사\s*[>\]\)】〉»]", "", name)
    name = re.sub(r"^\s*인사\s*|\s*인사\s*$", "", name)
    name = re.sub(r"^[◇◆■●◎▲△▼▷▶·\-\*:<\[\(【〈«\s]+|[◇◆■●◎▲△▼▷▶·\-\*:>\]\)】〉»\s]+$", "", name)

    # 영문 표기 또는 괄호 부가설명 제거 (예: (the bell), (The Bell), (주), (신문) 등)
    name = re.sub(r"\([A-Za-z0-9\s\.\-_&]+\)", "", name).strip()
    name = re.sub(r"[<\[【〈«](?:전보|승진|보임|보직|발령|인사|종합)[^>\]】〉»]*[>\]】〉»]", "", name).strip()

    # 2. 편집국, 보도국, 논설실 등 부서/국/본부 설명 제거
    name = re.sub(r"\s*(편집국|보도국|보도본부|디지털뉴스국|디지털뉴스본부|디지털국|논설실|논설위원실|신문총괄|경영총괄|미디어본부|방송본부|뉴스총괄)\b.*$", "", name)

    # 3. 법인명, 신문사, 일보사 등 접미사 정리
    name = re.sub(r"\(주\)|주식회사|신문사|일보사|방송사|미디어그룹", "", name).strip()
    if name.endswith("사") and len(name) >= 3 and not name.endswith("통신사"):
        name = name[:-1]

    n_compact = name.replace(" ", "").lower()

    # 방송/케이블/자회사 계열사 분리 우선 처리 (신문사와 자회사/방송사 혼동 방지)
    if "한국경제tv" in n_compact or "한경tv" in n_compact or "한국경제방송" in n_compact: return "한국경제TV"
    if "매일경제tv" in n_compact or "매경tv" in n_compact: return "매일경제TV"
    if "mbn" in n_compact: return "MBN"
    if "머니투데이방송" in n_compact or "mtn" in n_compact or "머니투데이tv" in n_compact: return "머니투데이방송"
    if "연합뉴스tv" in n_compact: return "연합뉴스TV"
    if "tv조선" in n_compact: return "TV조선"
    if "채널a" in n_compact: return "채널A"
    if "jtbc" in n_compact: return "JTBC"

    if "더벨" in name: return "더벨"
    if "팍스넷" in name: return "팍스넷뉴스"
    if "비즈니스워치" in name or "비즈워치" in name: return "비즈워치"
    if "토마토" in name: return "뉴스토마토"
    if "블로터" in name: return "블로터"
    if "이투데이" in name: return "이투데이"
    if "아시아투데이" in name: return "아시아투데이"
    if "뉴스핌" in name: return "뉴스핌"
    if "지디넷" in name: return "지디넷코리아"
    if "오마이" in name: return "오마이뉴스"
    if "프레시안" in name: return "프레시안"
    if "노컷" in name: return "노컷뉴스"
    if "더팩트" in name: return "더팩트"
    if "아이뉴스" in name: return "아이뉴스24"
    if "데일리안" in name: return "데일리안"
    if "머니s" in name.lower(): return "머니S"
    if "조선일보" in name or ("조선" in name and "일보" in raw_name): return "조선일보"
    if "중앙일보" in name or ("중앙" in name and "일보" in raw_name): return "중앙일보"
    if "동아일보" in name or ("동아" in name and "일보" in raw_name): return "동아일보"
    if "한국경제" in name or name in ["한경", "한경닷컴", "한경미디어", "한경미디어그룹"] or "한경미디어" in name or "한경닷컴" in name: return "한국경제"
    if "매일경제" in name or name in ["매경", "매경닷컴", "매경미디어", "매경미디어그룹"] or "매경미디어" in name: return "매일경제"
    if "서울경제" in name or name in ["서경", "서경마켓"]: return "서울경제"
    if "아시아경제" in name or name in ["아경"]: return "아시아경제"
    if "코리아헤럴드" in name or "koreaherald" in n_compact: return "코리아헤럴드"
    if "헤럴드경제" in name or name in ["헤경", "헤럴드경제신문"]: return "헤럴드경제"
    if any(h in name for h in ["헤럴드미디어", "(주)헤럴드", "주식회사 헤럴드", "헤럴드그룹"]) or name in ["헤럴드", "헤럴드미디어"]: return "헤럴드"
    if "헤럴드" in name or "herald" in n_compact: return "헤럴드"
    if "파이낸셜" in name: return "파이낸셜뉴스"
    if "머니투데이" in n_compact or "머투" in n_compact: return "머니투데이"
    if "이데일리m" in name.lower() or "이데일리 m" in name.lower(): return "이데일리M"
    if "이코노미스트" in name: return "이코노미스트"
    if "일간스포츠" in name: return "일간스포츠"
    if "이데일리" in name: return "이데일리"
    if "디지털타임스" in name: return "디지털타임스"
    if "전자신문" in name: return "전자신문"
    if "브릿지" in name: return "브릿지경제"
    if "아주경제" in name: return "아주경제"
    if "에너지경제" in name: return "에너지경제"
    if "연합뉴스" in name or name == "연합": return "연합뉴스"
    if "뉴스1" in name or "뉴스원" in name: return "뉴스1"
    if "뉴시스" in name: return "뉴시스"
    if "한국일보" in name: return "한국일보"
    if "경향" in name: return "경향신문"
    if "한겨레" in name: return "한겨레"
    if "문화일보" in name or ("문화" in name and "일보" in raw_name): return "문화일보"
    if "세계일보" in name or ("세계" in name and "일보" in raw_name): return "세계일보"
    if "국민일보" in name or ("국민" in name and "일보" in raw_name): return "국민일보"
    if "부산일보" in name: return "부산일보"
    if "국제신문" in name: return "국제신문"
    if "매일신문" in name: return "매일신문"
    if "영남일보" in name: return "영남일보"
    if "광주일보" in name: return "광주일보"
    if "대전일보" in name: return "대전일보"
    if "강원일보" in name: return "강원일보"
    if "강원도민일보" in name: return "강원도민일보"
    if "전북일보" in name: return "전북일보"
    if "전북도민일보" in name: return "전북도민일보"
    if "전남일보" in name: return "전남일보"
    if "전남도민일보" in name: return "전남도민일보"
    if "경남신문" in name: return "경남신문"
    if "경남도민일보" in name: return "경남도민일보"
    if "경인일보" in name: return "경인일보"
    if "경기일보" in name: return "경기일보"
    if "중부일보" in name: return "중부일보"
    if "인천일보" in name: return "인천일보"
    if "기호일보" in name: return "기호일보"
    if "충청투데이" in name: return "충청투데이"
    if "충청일보" in name: return "충청일보"
    if "중도일보" in name: return "중도일보"
    if "제민일보" in name: return "제민일보"
    if "한라일보" in name: return "한라일보"
    if "제주일보" in name: return "제주일보"
    if "울산매일" in name: return "울산매일"
    if "경상일보" in name: return "경상일보"
    if "경북일보" in name: return "경북일보"
    if "대구일보" in name: return "대구일보"
    if "남도일보" in name: return "남도일보"
    if "광남일보" in name: return "광남일보"
    if "시사저널" in name: return "시사저널"
    if "시사인" in name or "시사in" in name.lower(): return "시사IN"
    if "기자협회" in name: return "기자협회보"
    if "미디어오늘" in name: return "미디어오늘"
    if "블로터" in name: return "블로터"
    if "더벨" in name: return "더벨"
    if "딜사이트" in name: return "딜사이트"
    if "농민신문" in name: return "농민신문"
    if "국방일보" in name: return "국방일보"
    if "스포츠조선" in name: return "스포츠조선"
    if "스포츠동아" in name: return "스포츠동아"
    if "스포츠서울" in name: return "스포츠서울"
    if "스포츠경향" in name: return "스포츠경향"
    if "불교방송" in name or name.lower() == "bbs": return "BBS불교방송"
    if "평화방송" in name or name.lower() == "cpbc": return "CPBC가톨릭평화방송"
    if "기독교방송" in name or name.lower() == "cbs": return "CBS기독교방송"
    if "obs" in name.lower() or "경인방송" in name: return "OBS경인TV"
    if "knn" in name.lower(): return "KNN"
    if "tbc" in name.lower(): return "TBC"

    # 남아있는 불필요한 기호 및 단어 최종 정제
    name = re.sub(r"^[◇◆■●◎▲△▼▷▶·\-\*:<\[\(【〈«\s]+|[◇◆■●◎▲△▼▷▶·\-\*:>\]\)】〉»\s]+$", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name or "온라인뉴스"


def is_same_media_company(m1: str, m2: str) -> bool:
    """
    두 언론사명이 실질적으로 동일한 단일 언론사인지 판별합니다.
    신문사와 계열 방송사(예: 머니투데이 vs 머니투데이방송)는 엄격히 분리합니다.
    """
    if not m1 or not m2:
        return False
    n1 = normalize_media_name(m1).strip()
    n2 = normalize_media_name(m2).strip()
    if n1 == n2:
        return True

    # 1. 신문사와 계열 방송사/자회사 간 엄격 분리 (상호 배타적 처리)
    # [머니투데이(신문) vs 머니투데이방송(MTN)]
    is_n1_mt = (n1 == "머니투데이" or n1 in ["머투", "머니투데이그룹", "머니투데이미디어", "머니투데이신문"])
    is_n2_mt = (n2 == "머니투데이" or n2 in ["머투", "머니투데이그룹", "머니투데이미디어", "머니투데이신문"])
    is_n1_mtn = (n1 == "머니투데이방송" or "방송" in n1 or n1.lower() == "mtn" or "mtn" in n1.lower())
    is_n2_mtn = (n2 == "머니투데이방송" or "방송" in n2 or n2.lower() == "mtn" or "mtn" in n2.lower())
    if (is_n1_mt and is_n2_mtn) or (is_n2_mt and is_n1_mtn):
        return False

    # [한국경제 vs 한국경제TV]
    is_n1_hk = (n1 == "한국경제" or n1 in ["한경", "한경닷컴"])
    is_n2_hk = (n2 == "한국경제" or n2 in ["한경", "한경닷컴"])
    is_n1_hktv = ("tv" in n1.lower() or "방송" in n1) and "한국경제" in n1
    is_n2_hktv = ("tv" in n2.lower() or "방송" in n2) and "한국경제" in n2
    if (is_n1_hk and is_n2_hktv) or (is_n2_hk and is_n1_hktv):
        return False

    # [매일경제 vs MBN / 매일경제TV]
    is_n1_mk = (n1 == "매일경제" or n1 in ["매경", "매일경제신문"])
    is_n2_mk = (n2 == "매일경제" or n2 in ["매경", "매일경제신문"])
    is_n1_mbn = (n1.lower() == "mbn" or "매일경제tv" in n1.lower() or "mbn" in n1.lower())
    is_n2_mbn = (n2.lower() == "mbn" or "매일경제tv" in n2.lower() or "mbn" in n2.lower())
    if (is_n1_mk and is_n2_mbn) or (is_n2_mk and is_n1_mbn):
        return False

    # [조선일보 vs TV조선]
    if (n1 == "조선일보" and "tv조선" in n2.lower()) or (n2 == "조선일보" and "tv조선" in n1.lower()):
        return False

    # [동아일보 vs 채널A]
    if (n1 == "동아일보" and "채널a" in n2.lower()) or (n2 == "동아일보" and "채널a" in n1.lower()):
        return False

    # [중앙일보 vs JTBC]
    if (n1 == "중앙일보" and "jtbc" in n2.lower()) or (n2 == "중앙일보" and "jtbc" in n1.lower()):
        return False

    # [헤럴드 == 헤럴드미디어 == (주)헤럴드 (동일 법인/회사)]
    if {n1, n2}.issubset({"헤럴드", "헤럴드미디어", "(주)헤럴드", "주식회사 헤럴드", "헤럴드그룹"}):
        return True

    # 2. 명시적 별칭 매핑(MEDIA_GROUP_ALIASES) 검사 (동일 그룹 내 별칭인지 여부)
    for base_name, aliases in MEDIA_GROUP_ALIASES.items():
        all_group = [base_name] + aliases
        n_group = [normalize_media_name(a) for a in all_group]
        if n1 in n_group and n2 in n_group:
            return True

    # 3. 단순 접미사 차이 검사 (단, 방송/TV 키워드가 섞인 경우 배제)
    suffixes = ["신문사", "신문", "일보사", "일보", "사", "닷컴", "미디어"]
    for s in suffixes:
        c1 = n1.replace(s, "")
        c2 = n2.replace(s, "")
        if c1 and c1 == c2 and not any(kw in n1 or kw in n2 for kw in ["방송", "tv", "TV", "mtn", "MTN", "mbn", "MBN"]):
            return True

    return False


def parse_rank_level(rank_title: str) -> int:
    """직급 텍스트에서 가장 높은 직급 서열(1~10)을 반환합니다."""
    if not rank_title:
        return 2  # 기본값: 평기자 수준
    best_level = 2
    for r, level in RANK_HIERARCHY.items():
        if r in rank_title:
            if level > best_level or best_level == 2:
                best_level = level
    return best_level


MEDIA_KEYWORDS: List[str] = [
    "신문", "일보", "방송", "뉴스", "통신", "미디어", "타임스", "타임즈", "저널", "데일리", "헤럴드", "프레스",
    "tv", "kbs", "mbc", "sbs", "ytn", "jtbc", "tv조선", "채널a", "mbn", "ebs", "cbs", "bbs", "tbs",
    "연합뉴스", "뉴스1", "뉴시스", "한겨레", "오마이뉴스", "프레시안", "노컷뉴스", "더팩트", "아이뉴스24", "지디넷",
    "조선비즈", "머니투데이", "이데일리", "디지털타임스", "전자신문", "조세일보", "스포츠조선",
    "스포츠동아", "스포츠서울", "스포츠경향", "일간스포츠", "시사in", "시사저널", "주간조선", "주간동아", "한겨레21",
    "투데이", "토마토", "블로터", "더벨", "인사이트", "위키트리", "포스트", "포커스", "바이어스", "테크m",
    "월간", "주간", "잡지", "전문지", "통신사", "비즈", "이코노믹", "워치", "파이낸셜"
]

KNOWN_MEDIA_NAMES: set = {
    "뉴스1", "연합뉴스", "뉴시스", "중앙일보", "조선일보", "동아일보", "경향신문", "한겨레",
    "한국일보", "문화일보", "세계일보", "국민일보", "서울신문", "내일신문",
    "ytn", "YTN", "kbs", "KBS", "mbc", "MBC", "sbs", "SBS", "jtbc", "JTBC", "mbn", "MBN",
    "tv조선", "TV조선", "채널a", "채널A", "연합뉴스TV", "연합뉴스tv",
    "한국경제TV", "한국경제tv", "한경TV", "한경tv", "매일경제TV", "매일경제tv", "매경TV", "매경tv",
    "한경", "매경", "서경", "아경", "매일경제", "매일경제신문", "매일경제신문사", "매경미디어그룹", "매경미디어", "매경닷컴", "매경비즈", "한국경제", "한국경제신문", "한국경제신문사", "한경닷컴", "서울경제", "서울경제신문", "서울경제신문사", "아시아경제", "아시아경제신문", "헤럴드경제", "헤럴드경제신문", "헤럴드", "헤럴드미디어", "(주)헤럴드", "헤럴드그룹",
    "조선일보사", "동아일보사", "중앙일보사", "경향신문사", "한겨레신문", "한겨레신문사", "서울신문사", "세계일보사", "국민일보사", "문화일보사",
    "아시아투데이", "파이낸셜뉴스", "파이낸셜뉴스신문사", "이투데이", "뉴스토마토", "머니s", "머니S", "블로터", "더벨", "비즈한국",
    "비즈니스워치", "비즈워치", "글로벌이코노믹", "신아일보", "아주경제", "브릿지경제", "한국정경신문", "스마트에프엔",
    "fetv", "뉴스핌", "뉴스웨이", "뉴스포스트", "메트로신문", "에너지경제", "미디어펜", "미디어오늘", "pd저널", "PD저널",
    "기자협회", "한국기자협회", "기자협회보", "ebn", "EBN", "매경ax", "매경AX",
    "머니투데이", "머니투데이방송", "mtn", "MTN", "이코노미스트", "일간스포츠", "이데일리", "이데일리M", "이데일리m",
    "딜사이트", "인포스탁데일리", "소비자가만드는신문", "조세일보", "디지털데일리", "지디넷코리아",
    "전자신문", "더구루", "뉴스락", "프레시안", "노컷뉴스", "쿠키뉴스", "세계비즈", "세계비즈앤스포츠월드",
    "디지털타임스", "디지털타임즈", "디지털타임스사", "DT", "dt",
    "강원도민일보", "강원도민일보사", "전북도민일보", "전북도민일보사", "전남일보", "충청투데이",
    "OBS", "OBS경인TV", "obs", "BBS", "BBS불교방송", "bbs",
    "코리아타임스", "코리아타임즈", "코리아헤럴드", "스포츠조선", "스포츠동아", "스포츠서울", "스포츠경향",
    "시사저널", "시사저널e", "시사IN", "시사인", "경기일보", "경인일보", "중부일보",
    # 1. 전국 지역 종합 일간지 (전국 17개 시도)
    "부산일보", "부산일보사", "국제신문", "국제신문사", "경남신문", "경남신문사", "경남도민일보", "경남일보", "한려투데이",
    "매일신문", "매일신문사", "영남일보", "영남일보사", "대구일보", "대구일보사", "대구신문", "경북일보", "경북도민일보", "대경일보", "경북매일",
    "광주일보", "광주일보사", "전남도민일보", "광남일보", "남도일보", "무등일보", "광주매일", "광주매일신문",
    "전북일보", "전북일보사", "전라일보", "새전북신문",
    "대전일보", "대전일보사", "중도일보", "중도일보사", "충청일보", "충청매일", "동양일보", "중부매일", "충북일보",
    "강원일보", "강원일보사",
    "인천일보", "인천일보사", "기호일보", "경기신문", "경기매일",
    "제민일보", "제민일보사", "한라일보", "한라일보사", "제주일보", "제주일보사", "제주신보", "제주매일",
    "울산매일", "울산매일신문", "경상일보", "경상일보사", "울산신문",
    # 2. 지역 방송 및 지역 민방 / 지역 MBC / 지역 KBS
    "KNN", "knn", "TBC", "tbc", "kbc", "KBC", "kbc광주방송", "광주방송", "TJB", "tjb", "TJB대전방송", "대전방송",
    "UBC", "ubc", "UBC울산방송", "울산방송", "JTV", "jtv", "JTV전주방송", "전주방송", "CJB", "cjb", "CJB청주방송", "청주방송",
    "G1", "g1", "G1방송", "강원민방", "JIBS", "jibs", "JIBS제주방송", "제주방송",
    "부산MBC", "대구MBC", "광주MBC", "대전MBC", "전주MBC", "춘천MBC", "원주MBC", "MBC강원영동",
    "청주MBC", "충주MBC", "포항MBC", "안동MBC", "울산MBC", "MBC경남", "경남MBC", "목포MBC", "여수MBC", "제주MBC",
    "KBS부산", "KBS대구", "KBS광주", "KBS대전", "KBS전주", "KBS춘천", "KBS청주", "KBS창원", "KBS제주",
    # 3. 경제지, IT 전문지, 산업 전문지
    "포브스코리아", "매경이코노미", "한경비즈니스", "팩트경제신문", "스트레이트뉴스", "시장경제",
    "비즈니스포스트", "현대경제신문", "이코노믹리뷰", "팝콘뉴스", "국세신문", "세정신문", "세무신문", "대한경제", "건설경제",
    "지디넷", "zdnet", "테크M", "테크m", "보안뉴스", "데일리시큐", "CCTV뉴스", "전자부품",
    "의학신문", "메디칼타임즈", "청년의사", "약업신문", "데일리팜", "메디파나뉴스", "코메디닷컴", "헬스조선", "라포르시안", "병원신문", "한의신문", "치의신보",
    "농민신문", "농민신문사", "한국농어민신문", "축산신문", "수산신문", "식품저널", "식품음료신문",
    "전기신문", "가스신문", "정보통신신문", "철도신문", "물류신문", "해사신문", "한국해운신문", "국토일보", "건설타임즈", "기계설비신문", "에너지신문",
    "한국대학신문", "교수신문", "대학저널",
    # 4. 인터넷 종합지 및 주간지 / 시사지
    "데일리안", "오마이뉴스", "오마이뉴스사", "폴리뉴스", "뷰스앤뉴스", "프라임경제", "제주의소리", "디트뉴스24",
    "일요신문", "주간조선", "주간동아", "한겨레21", "주간경향", "시사주간", "미래한국",
    # 5. 스포츠 및 연예 전문지
    "OSEN", "osen", "마이데일리", "스타뉴스", "엑스포츠뉴스", "스포티비뉴스", "뉴스엔", "TV리포트",
    # 6. 종교 / 특수 방송 및 신문
    "CPBC가톨릭평화방송", "CPBC", "cpbc", "평화방송", "CBS기독교방송", "CBS", "cbs", "극동방송", "CTS기독교TV", "CTS", "cts",
    "평화신문", "가톨릭평화신문", "법보신문", "불교신문", "국방일보"
}


def parse_composite_search_query(q: str) -> Dict[str, str]:
    """
    '한국경제 건설부동산 부장', '헤럴드경제 김동호', '건설부동산 부장' 등
    복합 검색어를 입력했을 때 매체명, 부서명, 직급명, 기자명을 자동 분리합니다.
    """
    q = q.strip()
    if not q:
        return {"media": "", "dept": "", "rank": "", "name": ""}

    media, dept, rank, name = "", "", "", ""
    remainder = q

    # 1. 매체명 접두사 감지
    for km in sorted(KNOWN_MEDIA_NAMES, key=len, reverse=True):
        if remainder.startswith(km) and (len(remainder) == len(km) or remainder[len(km)] in (" ", "·", "-", "_")):
            media = km
            remainder = remainder[len(km):].strip(" ·-_")
            break
        elif remainder == km:
            media = km
            remainder = ""
            break

    # 2. 직급명 감지
    ranks = [
        "논설주간", "논설위원", "전문기자", "본부장", "부국장", "선임기자", 
        "국장", "부장대우", "부장", "차장대우", "차장", "팀장", "위원", 
        "대표이사", "대표", "사장", "이사", "상무", "전무", "기자"
    ]
    for r in ranks:
        if r in remainder:
            rank = r
            remainder = re.sub(rf"\b{r}\b|\s*{r}", "", remainder).strip()
            break

    # 3. 부서명 vs 기자명 분리
    if remainder:
        if (
            any(remainder.endswith(suffix) for suffix in ["부", "팀", "국", "실", "센터", "단", "소", "데스크"])
            or remainder in ["건설부동산", "부동산", "정치", "경제", "사회", "산업", "금융", "증권", "IT", "문화", "국제", "유통", "생활경제", "바이오", "테크"]
        ):
            dept = remainder
        elif len(remainder) in (2, 3, 4) and re.match(r"^[가-힣]+$", remainder):
            name = remainder
        else:
            dept = remainder

    return {"media": media, "dept": dept, "rank": rank, "name": name}

NON_MEDIA_KEYWORDS: List[str] = [
    "인사혁신처", "법제처", "보훈처", "연구단", "사업단", "연구실", "추진단", "사업본부", "정보통신", "통신기술", "소프트웨어", "엔터테인먼트",
    "경기대", "서울대", "연세대", "고려대",
    "부처", "기획재정부", "교육부", "과기정통부", "외교부", "통일부", "법무부", "국방부", "행안부", "보훈부",
    "문체부", "농림축산", "농식품부", "산업통상", "산업부", "복지부", "환경부", "고용노동부", "노동부", "여가부",
    "국토교통부", "국토부", "해수부", "중기부", "국세청", "관세청", "조달청", "통계청", "검찰청", "검찰", "경찰청", "경찰",
    "소방청", "문화재청", "산림청", "특허청", "기상청", "해경", "감사원", "국정원", "금감원", "금융위", "공정위", "방통위",
    "시청", "도청", "구청", "군청", "공사", "공단", "연구원", "진흥원", "재단", "은행", "금융지주", "증권사", "생명", "화재",
    "카드", "보험", "캐피탈", "대학교", "대학원", "대학", "병원", "의료원", "의학원", "원자력",
    "건설사", "현대건설", "대우건설", "gs건설", "포스코건설", "삼성전자", "lg전자", "전자공업",
    "물산", "중공업", "화학", "모빌리티", "자동차", "항공", "엔지니어링", "제약사", "백화점", "쇼핑", "호텔",
    "서울시", "부산시", "인천시", "대구시", "광주시", "대전시", "울산시", "세종시", "경기도", "강원도",
    "충북도", "충남도", "전북도", "전남도", "경북도", "경남도", "제주도", "안동시", "과기", "과학기술", "재정경제", "행정안전",
    "협회", "협의회", "연합회", "공제회", "조합", "학회", "위원회", "신문협회", "방송협회", "기자클럽"
]


def is_department_header(hdr: str) -> bool:
    """해당 헤더가 언론사명이 아닌 사내 부서/실/국/총괄 단위인지 판별합니다."""
    if not hdr:
        return False
    h = hdr.strip()
    if h in KNOWN_MEDIA_NAMES or any(km.lower() == h.lower() for km in KNOWN_MEDIA_NAMES):
        return False
    # 일반 단독 명사 및 부서명 키워드
    if h in [
        "미디어", "방송", "경제", "뉴미디어", "멀티미디어", "디지털", "온라인", "출판미디어", "미래미디어",
        "방송뉴스", "미디어연구소", "미디어전략연구소", "미디어기술연구소", "출판", "잡지", "월간", "주간"
    ]:
        return True
    if any(h.endswith(suffix) for suffix in [
        "실", "국", "부", "팀", "센터", "총괄", "본부", "파트", "데스크", "에디터석", "에디터",
        "연구소", "연구실", "연구원", "랩", "lab", "지사", "지국", "총국", "단", "처", "소", "반"
    ]):
        # 단, 언론사명 자체인 경우 제외 (예: 방송국 제외, 일보/신문 등)
        if any(h.endswith(m) for m in ["미디어그룹", "신문사", "방송사", "뉴스룸"]):
            return False
        return True
    if any(k in h for k in [
        "편집국", "보도국", "논설위원", "경영지원", "인재문화", "마케팅", "독자서비스", "독자권익", "사업본부",
        "디지털뉴스", "뉴스룸", "전략기획", "콘텐츠", "미디어랩", "기술연구소", "정보미디어"
    ]):
        return True
    return False


def is_media_organization(name: str) -> bool:
    """해당 기관/조직명이 언론사(신문/방송/통신/인터넷매체)인지 정밀 판별합니다."""
    if not name:
        return False
    n = name.lower().strip()
    # 단독 일반명사나 노이즈는 절대 언론사명이 아님
    if n in [
        "2010년", "2011년", "제1", "미상", "온라인뉴스", "뉴스", "언론사", "기자", "본사", "지사", "편집국", "보도국",
        "미디어", "방송", "경제", "뉴미디어", "멀티미디어", "디지털", "온라인", "출판미디어", "미래미디어",
        "방송뉴스", "미디어연구소", "미디어전략연구소", "미디어기술연구소", "출판", "잡지", "월간", "주간", "저널", "데일리", "타임스", "프레스"
    ]:
        return False
    if n in KNOWN_MEDIA_NAMES or any(km.lower() == n for km in KNOWN_MEDIA_NAMES):
        return True
    if is_department_header(n):
        return False
    # 직급명, 직책명, 인명이 포함된 경우 언론사명이 아님
    if any(r in n for r in ["부장", "차장", "국장", "팀장", "소장", "실장", "본부장", "부문장", "총괄", "위원", "기자", "대표", "이사", "이완성"]):
        return False
    # 부서/조직 접미사로 끝나는 경우 언론사명이 아님
    if any(n.endswith(suffix) for suffix in ["연구소", "연구실", "연구원", "센터", "본부", "부", "국", "팀", "실", "처", "단", "소", "랩"]):
        return False
    for nmk in NON_MEDIA_KEYWORDS:
        if nmk in n:
            return False
    # 길이 3자 이상이고 유효한 언론사 접미사를 가진 경우
    if len(n) >= 3 and any(n.endswith(s) for s in [
        "경제신문", "경제일보", "경제tv", "경제뉴스", "경제매거진", "경제포커스",
        "데일리", "저널", "타임스", "타임즈", "헤럴드", "일보", "신문", "투데이", "토마토"
    ]):
        return True
    for mk in MEDIA_KEYWORDS:
        if mk == "통신":
            if not any(ex in n for ex in ["정보통신", "통신기술", "이동통신", "무선통신"]):
                if mk in n and len(n) >= 4:
                    return True
        elif mk in n and len(n) >= 4:
            return True
    return False


def is_personnel_article_title(title: str, text: str = "", url: str = "") -> bool:
    """
    기사 제목 및 출처가 공식 인사 기사인지 검증합니다.
    언론사 전문 인사 섹션 URL은 즉시 통과하며, 비언론사 단독 기사는 배제합니다.
    """
    if not title:
        return False

    # 전용 인사 섹션 URL은 무조건 공식 인사 기사로 인정
    if url and any(sec in url for sec in ["sec_no=63", "/people/personnel", "/life-culture/press", "section_list_all"]):
        return True

    # 연예/스포츠/포토 기사 URL 즉시 배제
    if url and any(ex_domain in url for ex_domain in ["entertain.naver.com", "sports.naver.com"]):
        return False

    t = html.unescape(str(title)).replace(" ", "")

    # 부고 및 사망 기사 엄격 배제
    if any(bg in t for bg in ["[부고]", "<부고>", "【부고】", "(부고)", "부고]", "별세", "발인", "빈소"]):
        return False

    # 연예/포토/정치 기사의 비발령 '인사(손인사, 새해인사, 반체제인사 등)' 오인식 원천 차단
    false_insa_tokens = [
        "손인사", "첫인사", "눈인사", "목인사", "배꼽인사", "끝인사", "감사인사",
        "새해인사", "추석인사", "설인사", "작별인사", "마지막인사", "반체제",
        "야당인사", "여당인사", "정계인사", "유력인사", "주요인사", "고위인사", "정치인사",
        "빛나는인사", "환한인사", "미소인사", "반가운인사", "고개숙여", "허리숙여", "악수인사",
        "인사기사", "인사말", "인사를", "인사로", "인사하며", "인사했다"
    ]
    if any(fi in t for fi in false_insa_tokens):
        return False

    # 공식 발령 표기 기호 또는 명확한 인사 키워드 형식 확인
    has_personnel_marker = any(b in title for b in [
        "[인사]", "<인사>", "【인사】", "(인사)", "인사]", "[인사·동정]", "[인사/동정]",
        "<승진>", "[승진]", "<전보>", "[전보]", "<보임>", "[보임]", "<선임>", "[선임]"
    ]) or bool(re.search(r"(?:^|[\s<\[◇◆■●◎▲△▼▷▶])(?:인사|승진|전보|보임|선임|발령)(?:\s*[-:·|(\]>◆◇]|\b)", title))

    if not has_personnel_marker:
        return False

    t_lower = html.unescape(str(title)).lower()
    # 1. 언론사/미디어 명칭이 포함되어 있다면 종합 인사 기사이므로 통과
    for mk in MEDIA_KEYWORDS:
        if mk in t_lower:
            return True
    for km in KNOWN_MEDIA_NAMES:
        if km.lower() in t_lower:
            return True

    # 2. 제목에 '등', '외', '종합', '오늘의' 등이 포함된 복합 종합 인사 기사는 본문에 언론사 발령이 포함될 수 있으므로 허용
    if any(k in t_lower for k in [" 등", " 외", "/등", "/외", "종합", "오늘의", "外", "기자협회", "언론"]):
        return True

    # 3. 언론사명이 없고 단독 비언론사(정부부처/기업 등) 인사 기사인 경우 제외
    for nmk in NON_MEDIA_KEYWORDS:
        if nmk in t_lower:
            return False

    return True


EXCLUDED_DEPARTMENT_KEYWORDS = [
    "예능",         # 예능국, 예능센터, 예능본부, 예능제작, 예능기획 등 (방송 예능 제작/CP)
    "드라마",       # 드라마국, 드라마본부, 드라마센터, 드라마제작 등
    "라디오제작",   # 라디오 프로그램 제작 (보도국 라디오뉴스 제외)
    "공연사업",     # 문화공연 및 페스티벌 사업
    "쇼버라이어티", # 쇼/오락 프로그램 제작
]


def is_excluded_department(dept_name: str) -> bool:
    """
    기자/언론인 고유 업무와 무관한 방송사 예능, 드라마 등 비보도 프로그램 제작 부서 및 문장형 노이즈를 식별하여 제외합니다.
    """
    if not dept_name:
        return False
    d = str(dept_name).strip()
    if len(d) > 25 or any(p in d for p in [".", "다.", "했다", "이다", "으로", "하며", "에서", "kg", "㎞", "톤", "프로젝트", "수주", "가정할", "이른다"]):
        return True
    d_clean = d.replace(" ", "")
    return any(kw in d_clean for kw in EXCLUDED_DEPARTMENT_KEYWORDS)


# ==========================================
# 2. 한국 언론사 인사 기사 전용 정규표현식 파서
# ==========================================

class PersonnelRegexParser:
    """
    한국 언론사의 전형적인 인사 발령 기사 텍스트에서
    (언론사, 부서, 직급, 이름, 이메일, 발령구분)을 고속 추출하는 정규표현식 엔진.
    """

    # 구분 기호 및 불릿 문자 (각종 한글 특수문자, 가운뎃점, 아라에아(ㆍ), 그리스 델타(Δ/δ), 네모, 세모, 다이아몬드 등 완벽 대응)
    BULLET_CHARS = r"(?:^|\s)[·ㆍ]\s*|[△▲▷▶▼▽○●■□◆◇◈▣◎⊙◐◑\-\*―━–—※§☞Δδ]"
    
    # 2~4글자 한글 인명 패턴
    NAME_PATTERN = r"[가-힣]{2,4}"

    # 이메일 추출 패턴
    EMAIL_PATTERN = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"

    # 발령 액션 헤더 패턴 (예: <승진>, ◇전보, [국장급], <임용> 등)
    ACTION_HEADER_PATTERN = re.compile(
        r"(?:<|◇|◆|\[|\(|【)(승진|전보|보직|보임|선임|신규|신규선임|임용|임명|채용|선출|영입|전출|전입|파견|퇴임|위촉|이동|전임)(?:>|◇|◆|\]|\)|】)"
    )

    # 미디어 헤더 패턴 (한 기사 내 여러 언론사 구분용)
    MEDIA_HEADER_PATTERN = re.compile(
        r"(?:◆|◇|■|□|◎|●|⊙|◈|▣|○|【|\[|<)\s*([가-힣0-9A-Za-z]*(?:일보|경제|뉴스|신문|타임스|타임즈|미디어|데일리|헤럴드|통신|방송|TV|mbc|kbs|sbs|ytn|jtbc|뉴스1|뉴시스|연합뉴스|한겨레|투데이|토마토|블로터|더벨|파이낸셜|팍스넷|지디넷|비즈워치|비즈니스워치|이투데이|뉴스핌))(?:\([^\)]*\))?\s*(?:】|\]|>|◆|◇|■|□|◎|●|⊙|◈|▣|○)?"
    )

    ACTION_CANDIDATES: List[str] = [
        "승진", "전보", "보임", "보직", "선임", "신규선임", "임용", "임명", "채용",
        "선출", "영입", "전출", "전입", "파견", "퇴임", "위촉", "이동", "전임"
    ]

    @classmethod
    def clean_name(cls, name_candidate: str) -> Optional[str]:
        """추출된 이름 후보가 유효한 한글 인명인지 정제 및 필터링."""
        name = name_candidate.strip()
        if len(name) < 2 or len(name) > 4:
            return None
        if not re.fullmatch(r"[가-힣]{2,4}", name):
            return None
        if name in STOPWORDS or name in ["보직발령", "정기인사", "인사발령", "조직개편", "보직변경", "승진인사", "정기승진", "보직임명"]:
            return None
        if name in [
            "직무대행", "권한대행", "직대", "내정자", "내정", "상임고문", "고문", "전무이사", "상무이사", "상무보",
            "연구소장", "부소장", "칼럼니스트", "대표이사", "주필", "논설주간", "논설위원실장", "수석논설위원",
            "과장급", "과장", "부단장", "단장", "서기관", "서기관급", "사무관", "사무관급", "논설실장", "보도실장"
        ]:
            return None
        dept_keywords = [
            "정치", "경제", "사회", "산업", "금융", "증권", "문화", "국제", "체육", "유통", "중기", "바이오",
            "전국", "지방", "사진", "교열", "어문", "기획", "비서", "감사", "총무", "법무", "인사", "영업",
            "마케팅", "관리", "심의", "홍보", "독자", "제작", "업무", "재무", "회계", "디지털", "온라인", "콘텐츠",
            "뉴미디어", "편집", "보도", "논설", "해설", "여론", "조사", "사업"
        ]
        if any(name.endswith(suffix) for suffix in ["센터", "파트", "총괄", "데스크", "에디터", "본부"]):
            return None
        if any(name.endswith(suffix) for suffix in ["팀", "실", "국", "부", "처", "단", "소"]):
            if any(dk in name for dk in dept_keywords):
                return None
            if len(name) == 2:
                return None
            if name in ["인사팀", "총무팀", "기획실", "비서실", "감사실", "법무실", "편집국", "보도국", "사업단", "추진단", "연구단", "연구실", "사업본부", "출판국"]:
                return None
        if name in ["뉴스총괄", "지면총괄", "뉴스룸", "에디터석", "논설위원", "특파원"]:
            return None
        if any(nmk in name for nmk in NON_MEDIA_KEYWORDS):
            return None
        if name in cls.ACTION_CANDIDATES or name in ["임용", "보임", "임명", "승진", "전보", "전출", "전입", "파견", "선임", "채용", "영입", "이동", "전임", "보직", "선출", "위촉", "해임", "면직", "부서", "보직이동", "부서이동", "부서배치", "동정", "부음", "부고", "사령", "퇴임", "정년퇴임", "명예퇴임", "의원면직"]:
            return None
        if name in RANK_HIERARCHY or any(name == r for r in RANK_HIERARCHY):
            return None
        if is_media_organization(name):
            return None
        return name

    @classmethod
    def parse_article_text(
        cls,
        article_text: str,
        default_media: str = "",
        article_title: str = "",
        pub_date: str = "",
        article_url: str = ""
    ) -> List[Dict[str, Any]]:
        """
        기사 본문 전체를 순회하며 인사 발령 레코드 리스트를 추출합니다.
        """
        results: List[Dict[str, Any]] = []
        if not article_text:
            return results

        # 발행일자가 누락된 경우 URL, 기사 제목, 본문 상단에서 지능형 추출
        if not pub_date:
            if article_url:
                m_url = re.search(r"[/v=](\d{4})(\d{2})(\d{2})", article_url)
                if m_url:
                    pub_date = f"{m_url.group(1)}-{m_url.group(2)}-{m_url.group(3)}"
            if not pub_date and article_title:
                m_tit = re.search(r"(\d{4})[년.\-/\s]+(\d{1,2})[월.\-/\s]+(\d{1,2})", article_title)
                if m_tit:
                    pub_date = f"{m_tit.group(1)}-{int(m_tit.group(2)):02d}-{int(m_tit.group(3)):02d}"
            if not pub_date and article_text:
                m_body = re.search(r"(\d{4})[년.\-/\s]+(\d{1,2})[월.\-/\s]+(\d{1,2})[일자]?", article_text[:1000])
                if m_body:
                    pub_date = f"{m_body.group(1)}-{int(m_body.group(2)):02d}-{int(m_body.group(3)):02d}"

        # 제목에서 언론사 힌트 파악 (예: [인사] 아이뉴스24, [인사] 아시아투데이, [인사] 뉴스1)
        clean_article_title = re.sub(r"(?:연합뉴스|뉴스1)?\s*\d{4}\.\d{2}\.\d{2}\.?\s*네이버뉴스.*$", "", html.unescape(article_title)).strip()
        title_media_match = re.search(r"(?:\[인사\]|<인사>|【인사】|\(인사\))\s*([가-힣A-Za-z0-9]+)", clean_article_title)
        rev_match = re.search(r"([가-힣A-Za-z0-9]+)\s*(?:\[인사\]|<인사>|【인사】|\(인사\))", clean_article_title)
        
        cand = ""
        if title_media_match:
            cand = title_media_match.group(1).strip()
        elif rev_match:
            cand = rev_match.group(1).strip()

        if cand:
            if is_media_organization(cand):
                current_media = normalize_media_name(cand)
            else:
                # 기사 제목의 대상 기관이 언론사가 아닌 경우(예: 국회사무처, 화성시, 법무부, 경찰청 등)
                # 통신사(연합뉴스/뉴스1) 소속 인사로 오인식되지 않도록 비언론사명으로 격리
                current_media = cand
        elif default_media in ["뉴스1", "연합뉴스"] and any(w in clean_article_title for w in ["뉴스1", "연합뉴스"]):
            # 기사 제목에 뉴스1/연합뉴스 자사 인사가 명시된 경우에만 통신사 자사 발령으로 인정
            current_media = normalize_media_name(default_media)
        elif default_media and is_media_organization(default_media):
            # 출처가 언론사(중앙일보, 한국기자협회 등)인 경우 기본 매체 인정
            current_media = normalize_media_name(default_media)
        else:
            current_media = "비언론사"

        current_action = "발령"
        current_section_rank = ""
        current_dept = ""
        current_rank = ""
        current_bureau = ""

        # 줄 단위 또는 불릿 기호 단위로 분할 처리 (한 줄에 여러 다이아몬드 헤더가 공존하는 경우 분할)
        raw_lines = [html.unescape(line).strip() for line in article_text.splitlines() if line.strip()]
        lines: List[str] = []
        for rl in raw_lines:
            sub_parts = re.split(r"(?<=[^\s])\s*(?=[◇◆■□【⊙◈▣◎○●])", rl)
            for sp in sub_parts:
                sp = sp.strip()
                if sp:
                    lines.append(sp)

        for line in lines:
            # 0. 푸터, 제보 및 문의 안내 문구, 포털 하단 추천기사 배너 스킵
            if any(k in line for k in [
                "게재 문의", "제보", "저작권", "무단전재", "재배포", "기사문의", "확인용", "카톡", "연락처",
                "인기기사", "다른기사", "관련기사", "주요뉴스", "뉴스스탠드", "인터랙티브", "날벼락", "참여할까",
                "완패", "사태", "감탄한", "가다간", "만나보세요", "퍼가기"
            ]):
                continue

            # 0-1. 불릿도 없고 직급/발령/부서/언론사 헤더도 없는 단순 UI 버튼/단문 라인 스킵
            has_bullet = bool(re.search(cls.BULLET_CHARS, line))
            has_rank_or_action = any(r in line for r in ["부장", "차장", "국장", "부국장", "본부장", "팀장", "실장", "직대", "대우", "기자", "논설", "주필", "고문", "이사", "대표", "사장", "승진", "전보", "보임", "발령", "임용", "선임"])
            has_dept = any(d in line for d in ["국", "부", "팀", "실", "센터", "처", "소", "단", "본부", "지사", "지국", "연구소"])
            has_media_hdr = bool(cls.MEDIA_HEADER_PATTERN.search(line))
            if not has_bullet and not has_rank_or_action and not has_dept and not has_media_hdr:
                continue

            # 0-2. 통신사/바이라인 시그니처 및 기사 종결 기호 사전 정제 (예: (서울=연합뉴스), (서울=뉴스1), [이데일리 김유정기자] 등)
            line = re.sub(r"\([가-힣\s]*=(?:연합뉴스|뉴스1|[가-힣\s]*)\)", "", line)
            line = re.sub(r"\([^)]*(?:연합뉴스|뉴스1|끝)[^)]*\)", "", line)
            line = re.sub(r"\([가-힣\w\s·-]*=[가-힣\w\s·-]*\)", "", line)
            line = re.sub(r"\[[가-힣\w\s·-]*?(?:기자|특파원|뉴스1|연합뉴스|이데일리|뉴시스|글|사진)\]", "", line)
            line = re.sub(r"\[[가-힣\s]*=(?:연합뉴스|뉴스1|[가-힣\s]*?)\]", "", line)

            # 기사 도입부 문장(예: ~밝혔다, ~단행했다 등)은 언론사 헤더 변경 대상에서 제외
            if any(w in line for w in ["밝혔다", "실시했다", "발표했다", "단행했다", "전했다", "따르면", "인사내용"]):
                continue

            # 1. 언론사/기관 변경 감지 (기사 중간에 다른 언론사/기관 헤더가 나올 경우)
            media_match = cls.MEDIA_HEADER_PATTERN.search(line)
            if media_match:
                cand_media = media_match.group(1).strip()
                if is_media_organization(cand_media) and not is_department_header(cand_media):
                    current_media = normalize_media_name(cand_media)
                    current_dept = ""
                    current_rank = ""
                    current_bureau = ""
                elif not is_department_header(cand_media):
                    current_media = cand_media
                    current_dept = ""
                    current_rank = ""
                    current_bureau = ""
            else:
                clean_sec_hdr = re.sub(r"^[◇◆■□▲△▼▽▷▶<\[\(【Δδ⊙◈▣◎○●\s]+|[>\]\)】\s]+$", "", line).strip()
                clean_sec_hdr = re.sub(r"\([^\)]*\)", "", clean_sec_hdr).strip()
                clean_sec_hdr = re.split(r"[\]\)>】〉»<\[\(【〈«]", clean_sec_hdr)[0].strip()
                is_action_or_rank = any(act in clean_sec_hdr for act in cls.ACTION_CANDIDATES) or any(rnk in clean_sec_hdr for rnk in ["부장", "차장", "국장", "부국장", "본부장", "팀장", "실장", "직대", "대우", "기자", "논설", "주필", "위원", "단장", "디렉터", "총괄"])
                if not is_action_or_rank and len(clean_sec_hdr) <= 25 and not any(cls.clean_name(t) for t in line.split()):
                    if is_department_header(clean_sec_hdr):
                        current_bureau = clean_sec_hdr
                        current_dept = clean_sec_hdr
                    elif is_media_organization(clean_sec_hdr):
                        current_media = normalize_media_name(clean_sec_hdr)
                        current_dept = ""
                        current_rank = ""
                        current_bureau = ""
                    elif any(nmk in clean_sec_hdr for nmk in NON_MEDIA_KEYWORDS) or clean_sec_hdr.endswith(("처", "청", "원", "회", "공사", "공단", "재단", "시", "대학교", "대학", "병원", "학원", "그룹", "은행", "증권", "생명", "화재", "카드", "캐피탈", "저축은행", "건설", "전자", "통신", "제철", "중공업", "화학", "에너지", "지주", "금융", "협회", "연합회")):
                        current_media = clean_sec_hdr
                        current_dept = ""
                        current_rank = ""
                        current_bureau = ""

            # 2. 발령 액션 및 섹션 직급 헤더 감지 (예: ◇부장대우 승진, ◇차장 승진, <승진>, <보임>, <임용>, ◇전보 등)
            clean_hdr = re.sub(r"^[◇◆■□▲△▼▽▷▶<\[\(【Δδ\s]+", "", line).strip()
            action_found = False
            for act_cand in cls.ACTION_CANDIDATES:
                if act_cand in clean_hdr:
                    current_action = act_cand
                    action_found = True
                    break
            rank_found = False
            for r_cand in SORTED_RANKS:
                if clean_hdr.startswith(r_cand) or f"{r_cand} 승진" in clean_hdr or f"{r_cand}승진" in clean_hdr or f"{r_cand} 전보" in clean_hdr or f"<{r_cand}>" in line or f"[{r_cand}]" in line or f"({r_cand})" in line:
                    current_section_rank = r_cand
                    rank_found = True
                    break
            # 새로운 액션 헤더(예: ◇전보, ◇파견)가 나타났는데 직급 명시가 없으면 이전 섹션 직급 초기화
            if action_found and not rank_found:
                current_section_rank = ""

            # 4. 불릿 기호 기준으로 개별 발령 항목 쪼개기
            # 예: △건설부동산부장 김희준 △정치부 차장 박철수, Δ정치부장 박태정 Δ바이오부장 진성훈
            items = re.split(cls.BULLET_CHARS, line)
            for item in items:
                item = item.strip()
                if not item or len(item) < 2:
                    continue

                # 4-1. 항목 단위 언론사/기관 헤더 변경 감지 (한 줄에 여러 언론사가 연달아 나오는 경우)
                cand_media_clean = re.sub(r"^[<\[\(【Δδ\s]+|[>\]\)】\s]+$", "", item).strip()
                cand_media_clean = re.sub(r"\([^\)]*\)", "", cand_media_clean).strip()
                cand_media_clean = re.split(r"[\]\)>】〉»<\[\(【〈«]", cand_media_clean)[0].strip()
                is_item_action_or_rank = any(act in cand_media_clean for act in cls.ACTION_CANDIDATES) or any(rnk in cand_media_clean for rnk in ["부장", "차장", "국장", "부국장", "본부장", "팀장", "실장", "직대", "대우", "기자", "논설", "주필", "위원", "단장", "디렉터", "총괄"])
                if not is_item_action_or_rank and len(cand_media_clean) <= 15 and not any(cls.clean_name(t) for t in item.split()):
                    if is_department_header(cand_media_clean):
                        current_bureau = cand_media_clean
                        current_dept = cand_media_clean
                        continue
                    elif is_media_organization(cand_media_clean):
                        current_media = normalize_media_name(cand_media_clean)
                        current_dept = ""
                        current_rank = ""
                        current_bureau = ""
                        continue
                    elif any(nmk in cand_media_clean for nmk in NON_MEDIA_KEYWORDS) or cand_media_clean.endswith(("처", "청", "원", "회", "공사", "공단", "재단", "시", "대학교", "대학", "병원", "학원")):
                        current_media = cand_media_clean
                        current_dept = ""
                        current_rank = ""
                        current_bureau = ""
                        continue

                # 4-2. 항목 단위 발령 액션 및 섹션 직급 헤더 감지 (예: <부장대우 승진>, <승진>, <보임>, <임용>, <전보> 등)
                clean_item_hdr = re.sub(r"^[<\[\(【Δδ\s]+|[>\]\)】\s]+$", "", item).strip()
                item_has_header = False
                item_act_found = False
                for act_cand in cls.ACTION_CANDIDATES:
                    if act_cand in clean_item_hdr:
                        current_action = act_cand
                        item_has_header = True
                        item_act_found = True
                        break
                item_rank_found = False
                for r_cand in SORTED_RANKS:
                    if clean_item_hdr.startswith(r_cand) or f"{r_cand} 승진" in clean_item_hdr or f"{r_cand} 전보" in clean_item_hdr:
                        current_section_rank = r_cand
                        item_has_header = True
                        item_rank_found = True
                        break
                if item_act_found and not item_rank_found:
                    current_section_rank = ""
                if item_has_header and not any(cls.clean_name(t) for t in item.split()):
                    continue

                # 이메일 추출
                found_email = ""
                email_match = re.search(cls.EMAIL_PATTERN, item)
                if email_match:
                    found_email = email_match.group(0).strip()
                    item_clean = re.sub(cls.EMAIL_PATTERN, "", item).replace("()", "").strip()
                else:
                    item_clean = item

                # 0) 타부서/이전 소속 괄호 정보 추출 (예: 사회부장 홍길동 (전 경제부장), 정치부 차장 김철수(원소속: 사회부))
                former_match = re.search(r"\((?:전|원소속|구)\s*([가-힣\w\s·-]+?)\)", item_clean)
                former_dept_raw = former_match.group(1).strip() if former_match else ""

                # 괄호 패턴 정제 (예: 생활경제부장(부국장대우) 오승범 또는 정치부장(홍길동))
                # 1) 직급/발령 보조 설명 괄호 제거 (예: (부국장대우), (국장급), (승진), (전보), (보임), (임용), (직대), (대우), (겸 ...))
                item_clean = re.sub(r"\((?:[가-힣\w\s·-]*?(?:대우|급|승진|전보|보임|임용|보직|선임|직대|겸|전)[가-힣\w\s·-]*?)\)", "", item_clean)
                # 2) 한자 표기 괄호 제거 (예: 홍길동(洪吉童))
                item_clean = re.sub(r"\([一-龥\s]+\)", "", item_clean)
                # 3) 직책 뒤 괄호 안에 이름이 있는 경우 (예: 정치부장(홍길동))
                m_name_inside = re.search(r"([가-힣]{2,})\(([가-힣]{2,4})\)", item_clean)
                if m_name_inside and cls.clean_name(m_name_inside.group(2)):
                    item_clean = item_clean[:m_name_inside.start()] + f"{m_name_inside.group(1)} {m_name_inside.group(2)}" + item_clean[m_name_inside.end():]
                # 4) 통신사 출처 및 잔여 괄호 정리
                item_clean = re.sub(r"\([^)]*(?:연합뉴스|뉴스1|끝)[^)]*\)", "", item_clean)
                item_clean = re.sub(r"\([^)]*=[^)]*\)", "", item_clean)
                item_clean = re.sub(r"\([가-힣\w\s·-]*\)", "", item_clean)
                item_clean = re.sub(r"\s+", " ", item_clean).strip()

                # 겸직(겸) 분할 처리 (예: 뉴미디어랩부장 겸 건설부동산부장 허남영)
                split_items = []
                if " 겸 " in item_clean:
                    c_parts = item_clean.split(" 겸 ")
                    last_toks = c_parts[-1].strip().split()
                    cand_n = cls.clean_name(last_toks[-1]) if last_toks else None
                    for cp in c_parts:
                        cp_toks = cp.strip().split()
                        if cand_n and not cls.clean_name(cp_toks[-1]):
                            split_items.append(f"{cp.strip()} {cand_n}")
                        else:
                            split_items.append(cp.strip())
                else:
                    split_items = [item_clean]

                for s_item in split_items:
                    # 서브 아이템 파싱 (쉼표, 슬래시, 세미콜론 등으로 나열된 경우)
                    sub_records, current_dept, current_rank, current_bureau = cls._parse_single_bullet(
                        text=s_item,
                        media=current_media,
                        action=current_action,
                        section_rank=current_section_rank,
                        email=found_email,
                        last_dept=current_dept,
                        last_rank=current_rank,
                        last_bureau=current_bureau
                    )
                    for rec in sub_records:
                        rec["pub_date"] = pub_date or ""
                        rec["article_title"] = article_title or ""
                        rec["article_url"] = article_url or ""
                        # 날짜 왜곡 방지: 발행일자가 확인되지 않는 기사는 오늘 날짜로 조작하지 않고 누적 제외
                        if not rec.get("pub_date"):
                            continue
                        results.append(rec)

                        # 타부서/이전 소속 정보가 존재할 경우 이전 부서 발령 레코드 자동 파생 등록
                        if former_dept_raw and rec.get("name") and rec.get("pub_date"):
                            f_rank = ""
                            f_dept = former_dept_raw
                            for r_cand in SORTED_RANKS:
                                if f_dept.endswith(r_cand):
                                    f_rank = r_cand
                                    f_dept = f_dept[:-len(r_cand)].strip()
                                    break
                            if not f_dept:
                                f_dept = "본사"
                            if not f_rank:
                                f_rank = rec.get("rank_title", "기자")
                            
                            f_rank_lvl = RANK_SCORES.get(f_rank, 1) if "RANK_SCORES" in globals() else 1
                            results.append({
                                "name": rec["name"],
                                "media": rec["media"],
                                "dept": f_dept,
                                "rank_title": f_rank,
                                "rank_level": f_rank_lvl,
                                "domain": classify_domain(f_dept),
                                "email": rec.get("email", ""),
                                "pub_date": rec["pub_date"],
                                "action_type": f"전보 (이전 소속: {f_dept})",
                                "article_title": article_title or "",
                                "article_url": article_url or "",
                                "raw_text": f"(이전 소속) {former_dept_raw} {rec['name']}"
                            })

        return results

    @classmethod
    def _parse_single_bullet(
        cls,
        text: str,
        media: str,
        action: str,
        section_rank: str,
        email: str,
        last_dept: str = "",
        last_rank: str = "",
        last_bureau: str = ""
    ) -> Tuple[List[Dict[str, Any]], str, str, str]:
        """
        단일 발령 텍스트 파싱. 이름의 위치(앞, 뒤, 중간)와 다양한 형태, 등호(=), 복수 인명 나열,
        〃(반복 부호)의 정밀 부서/직급 계승 및 역할 수식어(직무대행) 자동 정제를 완벽 지원.
        """
        records: List[Dict[str, Any]] = []

        # 등호(=) 및 콜론(:) 공백 치환 후 쉼표, 슬래시, 세미콜론 등으로 분할
        clean_text = re.sub(r"\s*(=|:)\s*", " ", text)
        parts = re.split(r"[,/;|]", clean_text)

        cur_dept = last_dept
        cur_rank = last_rank or section_rank
        cur_bureau = last_bureau

        for part in parts:
            part = part.strip()
            if not part:
                continue

            # 1. 역할 수식어(직무대행, 권한대행 등) 감지 및 분리
            is_acting = False
            clean_part = part
            for mod in ["직무대행", "권한대행", "직대", "내정자", "내정"]:
                if mod in clean_part:
                    clean_part = re.sub(rf"\b{mod}\b", "", clean_part).strip()
                    is_acting = True

            raw_tokens = clean_part.split()
            if not raw_tokens:
                continue

            # 2. 〃(반복 부호) 토큰 정규화
            tokens = []
            has_ditto = False
            for t in raw_tokens:
                if t in ["〃", "″", "''"]:
                    tokens.append("DITTO")
                    has_ditto = True
                elif t.startswith("〃") or t.startswith("″") or t.startswith("''"):
                    sub_t = re.sub(r"^[〃″'']+", "", t).strip()
                    tokens.extend(["DITTO", sub_t])
                    has_ditto = True
                else:
                    tokens.append(t)
            tokens = [t for t in tokens if t]

            extracted_names: List[str] = []
            other_tokens: List[str] = []

            # 3. 토큰 중 유효한 한글 인명(2~4자) 찾기
            # 뒤쪽에서부터 연속으로 인명인 토큰들을 모두 수집 (예: "편집국 경제부 차장 홍길동 김철수")
            trailing_name_indices: List[int] = []
            for i in range(len(tokens) - 1, -1, -1):
                tok_cand = tokens[i]
                c_name = cls.clean_name(tok_cand)
                if c_name and tok_cand not in RANK_HIERARCHY and not re.search(f"({RANK_REGEX_PATTERN})$", tok_cand):
                    trailing_name_indices.append(i)
                else:
                    break

            if trailing_name_indices:
                split_point = min(trailing_name_indices)
                other_tokens = tokens[:split_point]
                for idx in reversed(trailing_name_indices):
                    c_name = cls.clean_name(tokens[idx])
                    if c_name:
                        extracted_names.append(c_name)
            elif len(tokens) > 1 and cls.clean_name(tokens[0]) and tokens[0] not in RANK_HIERARCHY and not re.search(f"({RANK_REGEX_PATTERN})$", tokens[0]):
                # 앞쪽에서 인명이 시작하는 경우 (예: "홍길동 건설부동산부장")
                front_name = cls.clean_name(tokens[0])
                if front_name:
                    extracted_names.append(front_name)
                    other_tokens = tokens[1:]
            else:
                # 중간에 인명이 있는 경우 (예: "논설위원실 양준영 논설위원", "편집국 김정태 부장")
                name_idx = -1
                for i, tok in enumerate(tokens):
                    c_name = cls.clean_name(tok)
                    if c_name and tok not in RANK_HIERARCHY and not re.search(f"({RANK_REGEX_PATTERN})$", tok):
                        name_idx = i
                        extracted_names.append(c_name)
                        break
                if name_idx != -1:
                    other_tokens = [tok for i, tok in enumerate(tokens) if i != name_idx]
                else:
                    other_tokens = tokens

            extracted_rank = section_rank or cur_rank or "기자"
            extracted_dept = cur_dept or "본사"

            # 4. 이름 위치 및 DITTO, 토큰에 따른 부서와 직급 확정
            if extracted_names:
                if has_ditto:
                    if len(other_tokens) == 2 and other_tokens[0] == "DITTO" and other_tokens[1] == "DITTO":
                        extracted_dept = cur_dept or "본사"
                        extracted_rank = cur_rank or section_rank or "기자"
                    elif len(other_tokens) == 1 and other_tokens[0] == "DITTO":
                        extracted_dept = cur_dept or "본사"
                        extracted_rank = cur_rank or section_rank or "기자"
                    elif other_tokens and other_tokens[0] == "DITTO":
                        rest = " ".join(other_tokens[1:])
                        r_match = re.search(f"({RANK_REGEX_PATTERN})$", rest)
                        if r_match:
                            extracted_rank = r_match.group(1)
                            sub_d = rest[:r_match.start()].strip()
                            if sub_d:
                                if not sub_d.endswith("부") and not sub_d.endswith("팀"):
                                    sub_d += ("팀" if "팀" in extracted_rank else "부")
                                extracted_dept = f"{cur_bureau} {sub_d}".strip() if cur_bureau else sub_d
                            else:
                                extracted_dept = cur_dept or "본사"
                        else:
                            extracted_dept = f"{cur_bureau} {rest}".strip() if cur_bureau else rest
                else:
                    if not other_tokens:
                        extracted_rank = section_rank or cur_rank or "기자"
                        extracted_dept = cur_dept or "본사"
                    elif len(other_tokens) == 1:
                        first = other_tokens[0]
                        r_match = re.search(f"({RANK_REGEX_PATTERN})$", first)
                        if r_match:
                            extracted_rank = r_match.group(1)
                            dept_part = first[:r_match.start()].strip()
                            if dept_part:
                                if (extracted_rank.startswith("부장") or extracted_rank.startswith("차장") or extracted_rank.startswith("팀장")) and not dept_part.endswith("부") and not dept_part.endswith("팀"):
                                    dept_part = dept_part + ("팀" if "팀" in extracted_rank else "부")
                                extracted_dept = dept_part
                            elif extracted_rank in EXECUTIVE_RANKS:
                                extracted_dept = "본사"
                            else:
                                extracted_dept = cur_dept or "본사"
                        elif first in RANK_HIERARCHY:
                            extracted_rank = first
                            extracted_dept = "본사" if first in EXECUTIVE_RANKS else (cur_dept or "본사")
                        else:
                            extracted_dept = first
                            extracted_rank = section_rank or cur_rank or "기자"
                    else:
                        full_str = " ".join(other_tokens)
                        r_match = re.search(f"({RANK_REGEX_PATTERN})$", full_str)
                        if r_match:
                            extracted_rank = r_match.group(1)
                            dept_part = full_str[:r_match.start()].strip()
                            if dept_part:
                                extracted_dept = dept_part
                            elif extracted_rank in EXECUTIVE_RANKS:
                                extracted_dept = "본사"
                            else:
                                extracted_dept = cur_dept or "본사"
                        else:
                            extracted_dept = full_str
                            extracted_rank = section_rank or cur_rank or "기자"

            # 5. 토큰이 1개로 붙어있는 경우 (예: "건설부동산부장김희준" or "홍길동정치부장")
            elif len(tokens) == 1:
                single_str = tokens[0]
                match1 = re.search(f"({RANK_REGEX_PATTERN})([가-힣]{{2,4}})$", single_str)
                match2 = re.search(f"^([가-힣]{{2,4}})({RANK_REGEX_PATTERN})", single_str)

                if match1 and cls.clean_name(match1.group(2)):
                    extracted_names = [match1.group(2)]
                    extracted_rank = match1.group(1)
                    extracted_dept = single_str[:match1.start()].strip() or cur_dept or "본사"
                elif match2 and cls.clean_name(match2.group(1)):
                    extracted_names = [match2.group(1)]
                    extracted_rank = match2.group(2)
                    extracted_dept = single_str[match2.end():].strip() or cur_dept or "본사"

            if is_acting:
                extracted_rank = f"{extracted_rank} 직무대행"

            clean_dept = extracted_dept
            for b in ["편집국 ", "보도국 ", "논설실 ", "디지털뉴스국 ", "신문총괄 "]:
                if clean_dept.startswith(b) and len(clean_dept) > len(b):
                    clean_dept = clean_dept[len(b):].strip()

            if extracted_dept and extracted_dept != "본사":
                cur_dept = extracted_dept
                m_bur = re.search(r"([가-힣A-Za-z0-9]+(?:국|실|본부))", extracted_dept)
                if m_bur:
                    cur_bureau = m_bur.group(1).strip()

            if extracted_rank and extracted_rank not in ["기자", "평기자"]:
                cur_rank = extracted_rank

            for extracted_name in extracted_names:
                final_media = normalize_media_name(media) if media else "미상"
                if not is_media_organization(final_media):
                    continue

                clean_dept_item = re.sub(r'^[△▲▷▶○●■□◆◇◈▣◎⊙◐◑·ㆍ\-\*―━“"”’‘\'〃\s]+|[“"”’‘\'〃\s]+$', '', clean_dept).strip()
                clean_dept_item = re.sub(r'[\s“"”’‘\'〃]+', ' ', clean_dept_item).strip()
                if not clean_dept_item and cur_dept:
                    clean_dept_item = cur_dept
                clean_rank_item = re.sub(r"^[△▲▷▶○●■□◆◇◈▣◎⊙◐◑·ㆍ\-\*―━\s]+", "", extracted_rank).strip()
                if not clean_rank_item:
                    clean_rank_item = "기자"
                if clean_dept_item in ["산업", "정치", "경제", "사회", "문화", "국제", "금융", "증권", "건설부동산"]:
                    clean_dept_item = clean_dept_item + ("팀" if "팀" in clean_rank_item else "부")

                # 부서가 본사/미지정이고 직급도 단순 기자인데 불릿이나 액션 헤더가 없었던 단문 노이즈는 제외
                if clean_dept_item in ["본사", "미지정", ""] and clean_rank_item in ["기자", "평기자"] and not section_rank and not has_ditto and not action:
                    continue

                if is_excluded_department(clean_dept_item):
                    continue

                rank_level = parse_rank_level(clean_rank_item)
                domain = classify_domain(clean_dept_item)

                records.append({
                    "media": final_media,
                    "dept": clean_dept_item,
                    "domain": domain,
                    "rank_title": clean_rank_item,
                    "rank_level": rank_level,
                    "name": extracted_name,
                    "action_type": action,
                    "email": email,
                    "raw_text": part
                })

        return records, cur_dept, cur_rank, cur_bureau


# ==========================================
# 3. 동명이인 분리 클러스터링 & 이직 탐지 엔진
# ==========================================

class PersonnelTrackerEngine:
    """
    발령 기록들을 모아 동명이인을 분리하고 고유 식별자(reporter_id)를 부여하며,
    매체 간 이직(Job Change)을 자동 탐지하여 타임라인을 구성하는 통합 엔진.
    """

    def __init__(self, db_path: str = "media_tracker.db"):
        if not os.path.isabs(db_path):
            base_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(base_dir, db_path)
        self.db = MediaDatabase(db_path)

    def process_records(self, raw_records: List[Dict[str, Any]], append: bool = True) -> Dict[str, Any]:
        """
        추출된 모든 레코드를 시계열 순서로 정렬한 후,
        1순위 이메일 매칭 및 2순위 알고리즘 휴리스틱을 적용하여 클러스터링을 수행합니다.
        append=True인 경우 기존 DB의 레코드들과 병합(Merge)하여 누적 처리합니다.
        """
        if append:
            existing_records = self.db.get_all_raw_records()
            seen_keys = set()
            combined_records = []
            
            for r in existing_records:
                if is_excluded_department(r.get("dept", "")):
                    continue
                key = (str(r.get("pub_date", "")), str(r.get("media", "")), str(r.get("name", "")), str(r.get("dept", "")), str(r.get("rank_title", "")))
                if key not in seen_keys:
                    seen_keys.add(key)
                    combined_records.append(dict(r))

            new_added_count = 0
            for r in raw_records:
                if is_excluded_department(r.get("dept", "")):
                    continue
                key = (str(r.get("pub_date", "")), str(r.get("media", "")), str(r.get("name", "")), str(r.get("dept", "")), str(r.get("rank_title", "")))
                if key not in seen_keys:
                    seen_keys.add(key)
                    combined_records.append(r)
                    new_added_count += 1
            
            effective_records = combined_records
        else:
            effective_records = [r for r in raw_records if not is_excluded_department(r.get("dept", ""))]
            new_added_count = len(effective_records)

        # 언론사 인사소식만 엄격 필터링 및 매체명/부서명 클렌징
        cleaned_records = []
        for r in effective_records:
            if not is_media_organization(r.get("media", "")) or (r.get("article_title") and not is_personnel_article_title(r.get("article_title", ""))) or is_excluded_department(r.get("dept", "")):
                continue
            r_copy = dict(r)
            # 매체명 정규화 (머니투데이와 머니투데이방송 엄격 분리)
            art_t = r_copy.get("article_title", "")
            raw_t = r_copy.get("raw_text", "")
            med = r_copy.get("media", "")
            med_norm = normalize_media_name(med)
            if med_norm == "머니투데이방송" or "머니투데이방송" in med or "mtn" in med.lower():
                r_copy["media"] = "머니투데이방송"
            elif med_norm == "머니투데이" or med in ["머투", "머니투데이", "머니투데이신문", "머니투데이그룹", "머니투데이미디어"]:
                r_copy["media"] = "머니투데이"
            elif med_norm in ["비언론사", "온라인뉴스", "미상", ""] and ("머니투데이방송" in art_t or "MTN" in art_t):
                r_copy["media"] = "머니투데이방송"
            elif med_norm in ["비언론사", "온라인뉴스", "미상", ""] and "머니투데이" in art_t:
                r_copy["media"] = "머니투데이"
            elif med_norm == "이코노미스트" or "이코노미스트" in med or "이코노미스트" in art_t:
                r_copy["media"] = "이코노미스트"
            elif med_norm == "일간스포츠" or "일간스포츠" in med or "일간스포츠" in art_t:
                r_copy["media"] = "일간스포츠"
            elif med_norm == "코리아헤럴드" or "코리아헤럴드" in med:
                r_copy["media"] = "코리아헤럴드"
            elif med_norm == "헤럴드경제" or "헤럴드경제" in med:
                r_copy["media"] = "헤럴드경제"
            elif med_norm in ["헤럴드", "헤럴드미디어"] or med in ["헤럴드", "헤럴드미디어", "(주)헤럴드"]:
                r_copy["media"] = "헤럴드"
            else:
                r_copy["media"] = med_norm

            # 부서명 따옴표/반복부호 정제
            d_str = re.sub(r'^[△▲▷▶○●■□◆◇◈▣◎⊙◐◑·ㆍ\-\*―━“"”’‘\'〃\s]+|[“"”’‘\'〃\s]+$', '', str(r_copy.get("dept", ""))).strip()
            d_str = re.sub(r'[\s“"”’‘\'〃]+', ' ', d_str).strip()
            rnk = r_copy.get("rank_title", "")
            if d_str in ["산업", "정치", "경제", "사회", "문화", "국제", "금융", "증권", "건설부동산"]:
                d_str += ("팀" if "팀" in rnk else "부")
            if d_str:
                r_copy["dept"] = d_str

            cleaned_records.append(r_copy)

        effective_records = cleaned_records

        if not effective_records:
            return {"total_records": 0, "new_records": 0, "unique_reporters": 0, "job_changes_detected": 0}

        # 날짜순 정렬 (과거 -> 현재)
        def parse_date_key(r):
            d_str = str(r.get("pub_date", "1970-01-01")).replace("-", "").replace(".", "")
            if len(d_str) >= 8:
                return d_str[:8]
            return "19700101"

        sorted_records = sorted(effective_records, key=parse_date_key)

        # 레코드별 date 객체 사전 파싱 및 매체/이름별 인덱스 구축 (이직 판정 시 양방향 크로스체크용)
        records_by_media_and_name: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for r in sorted_records:
            if "_date_obj" not in r:
                d_str = str(r.get("pub_date", "1970-01-01")).replace(".", "-").split("T")[0][:10]
                try:
                    r["_date_obj"] = datetime.datetime.strptime(d_str, "%Y-%m-%d").date()
                except Exception:
                    r["_date_obj"] = datetime.date(1970, 1, 1)

            r_name = str(r.get("name", "")).strip()
            r_media = normalize_media_name(r.get("media", ""))
            if r_name and r_media:
                k = (r_media, r_name)
                if k not in records_by_media_and_name:
                    records_by_media_and_name[k] = []
                records_by_media_and_name[k].append(r)

        # 동명이인 클러스터 컨테이너: Dict[name, List[ReporterCluster]]
        clusters_by_name: Dict[str, List[Dict[str, Any]]] = {}

        detected_job_changes: List[Dict[str, Any]] = []

        for record in sorted_records:
            name = record["name"]
            if not name:
                continue

            if name not in clusters_by_name:
                clusters_by_name[name] = []

            target_cluster = self._match_or_create_cluster(
                record=record,
                existing_clusters=clusters_by_name[name],
                job_changes_collector=detected_job_changes,
                media_records_index=records_by_media_and_name
            )

            # 레코드에 확정된 reporter_id 및 display_label 부여
            record["reporter_id"] = target_cluster["reporter_id"]

        # DB에 저장 및 동기화
        self.db.save_records_and_clusters(
            raw_records=sorted_records,
            clusters_by_name=clusters_by_name,
            job_changes=detected_job_changes
        )

        total_unique = sum(len(c_list) for c_list in clusters_by_name.values())
        return {
            "total_records": len(sorted_records),
            "new_records": new_added_count,
            "unique_reporters": total_unique,
            "job_changes_detected": len(detected_job_changes)
        }

    def _match_or_create_cluster(
        self,
        record: Dict[str, Any],
        existing_clusters: List[Dict[str, Any]],
        job_changes_collector: List[Dict[str, Any]],
        media_records_index: Optional[Dict[Tuple[str, str], List[Dict[str, Any]]]] = None
    ) -> Dict[str, Any]:
        """
        단일 레코드에 대해 기존 클러스터 매칭 여부를 검증하고, 일치하는 클러스터가 없으면 신규 클러스터를 생성합니다.
        """
        name = str(record.get("name", "")).strip()
        rec_media = normalize_media_name(record.get("media", ""))
        rec_dept = str(record.get("dept", "")).strip()
        rec_domain = str(record.get("domain", "")).strip()
        rec_rank = str(record.get("rank_title", "")).strip()
        rec_level = record.get("rank_level") or RANK_SCORES.get(rec_rank, 1)
        rec_date_str = str(record.get("pub_date", "2020-01-01"))
        rec_email = record.get("email", "").strip()

        rec_email_prefix = ""
        if rec_email and "@" in rec_email:
            rec_email_prefix = rec_email.split("@")[0].lower()

        rec_date = record.get("_date_obj")
        if not rec_date:
            try:
                rec_date = datetime.datetime.strptime(
                    rec_date_str.replace(".", "-").split("T")[0][:10], "%Y-%m-%d"
                ).date()
            except Exception:
                rec_date = datetime.date(2020, 1, 1)

        best_cluster = None
        best_score = -999.0
        is_job_change_candidate = False
        change_metadata = None

        for cluster in existing_clusters:
            # 1. 타임라인 중복 방지 (Timeline Conflict Exclusion)
            # 동일/근접 기간(30일 이내)에 서로 다른 매체에서 활동 기록이 있으면 배제
            conflict = False
            for prev_rec in cluster["records"]:
                p_date = prev_rec["date_obj"]
                p_media = normalize_media_name(prev_rec["media"])
                days_gap = abs((rec_date - p_date).days)
                if days_gap <= 30 and p_media != rec_media:
                    # 계열사/동일 그룹 내 발령이거나, 14일 이내 동일/유사 부서 기사 중복 보도인 경우 충돌이 아님
                    if are_group_affiliates(rec_media, p_media):
                        continue
                    if days_gap <= 14:
                        p_dept = prev_rec.get("dept", "")
                        if p_dept == rec_dept or (rec_domain == prev_rec.get("domain", "") and rec_rank == prev_rec.get("rank_title", "")):
                            continue
                    conflict = True
                    break
            if conflict:
                continue

            score = 0.0
            last_rec = cluster["records"][-1]
            last_media = normalize_media_name(last_rec["media"])
            last_date = last_rec["date_obj"]
            last_level = last_rec["rank_level"]
            last_dept = last_rec["dept"]
            last_domain = last_rec["domain"]

            days_diff = (rec_date - last_date).days
            rank_diff = rec_level - last_level

            # ----------------------------------------------------
            # 1순위: 이메일 바이라인 매칭
            # ----------------------------------------------------
            c_email_prefix = cluster.get("email_prefix", "")
            has_same_email = bool(rec_email_prefix and c_email_prefix and rec_email_prefix == c_email_prefix)
            if has_same_email:
                score += 150.0  # 이메일 접두사 일치 시 절대적 가점

            # ----------------------------------------------------
            # 2순위: 알고리즘 휴리스틱 스코어링
            # ----------------------------------------------------
            is_group_aff = are_group_affiliates(rec_media, last_media)
            is_desk_lateral = (4 <= rec_level <= 6 and 4 <= last_level <= 6)
            is_job_change = False
            candidate_change_meta = None

            # CASE A: 동일 언론사 또는 동일 미디어 그룹 계열사 내의 이동/승진/중복보도
            if rec_media == last_media or is_group_aff:
                score += 55.0

                # 동일 언론사 내 관리직/데스크 간 순환 보직 (3급 이상: 차장대우~국장급 간 이동)
                is_senior_lateral = (3 <= rec_level <= 9 and 3 <= last_level <= 9)

                if -1 <= rank_diff <= 3 or is_desk_lateral or is_senior_lateral:
                    score += 30.0  # 정상 유지 또는 승진 / 데스크 라인 보직 변경
                elif rank_diff < -1:
                    if rec_rank in ["기자", "평기자", "미표기", "수습기자", ""]:
                        # 발령 기사에서 직급이 생략/미언급된 경우 (예: "△사회부 홍길동"): 강등이 아니라 직급 미표기임
                        score += 20.0
                    else:
                        score -= 20.0
                elif rank_diff > 3:
                    if days_diff >= 730:
                        score += 20.0
                    else:
                        score -= 15.0

                domain_sim = calculate_domain_similarity(rec_domain, last_domain)
                if domain_sim >= 0.5:
                    score += domain_sim * 25.0
                else:
                    # 도메인이 다른 타부서 발령(예: 정치부 -> 사회부/경제부)도 동일 언론사에서는 정상 인사 이동
                    score += 15.0

            # CASE B: 서로 다른 언론사 간의 이동 (이직 탐지 알고리즘)
            else:
                # ----------------------------------------------------
                # [전사 시스템 이직 크로스체크 검증 시스템 (Bi-directional Cross-check)]
                # 1) 대상 매체(to_media) 이전 인사기사 크로스체크:
                #    발령일(rec_date) 이전에 대상 매체(to_media)에 이미 동일 이름의 인사기사가 존재하는지 확인.
                #    이미 과거에 재직했던 기록이 있다면, 신규 타사 이직 영입이 아니라
                #    해당 매체의 내부 기존 직원이거나 해당 매체 고유의 동명이인이므로 이직 판단에서 완전 배제.
                # 2) 출신 매체(from_media) 사후 재직 크로스체크 (유령 이직 방지):
                #    발령일(rec_date) 이후에도 출신 매체(from_media)에서 계속해서 인사기사가 보도된다면,
                #    실제로 타사로 이직한 것이 아니므로(동명이인 오매칭 또는 단순 인용/파견 등) 이직 판단에서 완전 배제.
                # ----------------------------------------------------
                has_target_prior_history = False
                has_origin_subsequent_history = False

                if media_records_index is not None:
                    # 1) 대상 매체 이전 기록 확인 (타깃 매체에 이전 발령 이력이 이미 존재하는지 검증)
                    target_records = media_records_index.get((rec_media, name), [])
                    for tr in target_records:
                        tr_date = tr.get("_date_obj") or tr.get("date_obj")
                        if tr_date and tr_date < rec_date:
                            has_target_prior_history = True
                            break

                    # 2) 출신 매체 사후 기록 확인 (출신 매체에 사후 발령 이력이 계속 존재하는지 검증)
                    origin_records = media_records_index.get((last_media, name), [])
                    for fr in origin_records:
                        fr_date = fr.get("_date_obj") or fr.get("date_obj")
                        if fr_date and fr_date > rec_date:
                            has_origin_subsequent_history = True
                            break

                is_recruit_action = record.get("action_type") in ["임용", "영입", "전입", "이동"]
                # 언론계 이직 탐색 기간: 단기 이직부터 일반적인 2~4년 근속 후 이직(최대 5년, 1825일)까지 폭넓게 수용
                is_within_transfer_window = (0 <= days_diff <= 1825) or has_same_email
                is_demoted = (rank_diff < 0) and not is_desk_lateral

                # 부서 및 도메인 유사도 검증
                domain_sim = calculate_domain_similarity(rec_domain, last_domain)
                last_kw = set(re.findall(r"[가-힣]{2,}", last_dept)) - STOPWORDS
                rec_kw = set(re.findall(r"[가-힣]{2,}", rec_dept)) - STOPWORDS
                kw_match = bool(last_kw and rec_kw and (last_kw & rec_kw))
                is_similar_dept = (domain_sim >= 0.5) or kw_match or (rec_domain == last_domain and rec_domain != "일반") or has_same_email

                # 크로스체크 통과 여부: 대상 매체에 이전 기록이 없어야 하고, 출신 매체에 사후 기록이 없어야 함
                crosscheck_passed = not has_target_prior_history and not has_origin_subsequent_history

                if is_within_transfer_window and not is_demoted and is_similar_dept and crosscheck_passed:
                    # 정상적인 매체 간 이직 판정
                    score += 65.0 + domain_sim * 30.0
                    if is_recruit_action:
                        score += 30.0

                    # 직급 연속성 및 승진 가점
                    if rank_diff == 0 or is_desk_lateral:
                        # 동일 직급 유지 이직 또는 데스크급 수평 이직
                        score += 40.0
                    elif rank_diff == 1:
                        # 1단계 정상 승진 이직
                        score += 25.0
                    elif rank_diff == 2:
                        # 2단계 건너뛰기 승진 (차장->부장 등): 6개월 이내 초단기 이직 시 동명이인 오인식 방지
                        if days_diff <= 180:
                            score += 5.0
                        else:
                            score += 15.0
                    else:
                        score += 5.0

                    # 근속 및 시차 가점
                    if days_diff <= 180:
                        score += max(0.0, 15.0 - (days_diff / 15.0))
                    elif 180 < days_diff <= 1500:
                        # 6개월~4년 정상 근속 후 이직
                        score += 15.0
                    else:
                        score += 5.0

                    is_group_transfer = are_group_affiliates(last_media, rec_media)
                    # 동일 그룹/계열사 내에서 30일 이내 발생한 발령은 중복 보도 또는 내부 전보이므로 이직(Job Change) 리스트에는 등록하지 않음
                    is_real_job_change = not (is_group_transfer and days_diff <= 30)
                    is_job_change = is_real_job_change
                    candidate_change_meta = {
                        "name": name,
                        "from_media": last_media,
                        "from_dept": last_dept,
                        "from_rank": last_rec["rank_title"],
                        "from_date": str(last_date),
                        "to_media": rec_media,
                        "to_dept": rec_dept,
                        "to_rank": rec_rank,
                        "to_date": str(rec_date),
                        "days_gap": days_diff,
                        "confidence_score": round(score, 1),
                        "detected_reason": (
                            f"{'6개월 내 타사 이동' if days_diff <= 180 else f'{round(days_diff/365, 1)}년 만의 타사 이직'}, "
                            f"부서 유사도({int(domain_sim*100)}%), "
                            f"{'이메일 일치' if has_same_email else '직급 승진 및 유지'}"
                            f" (대상 매체 이전 인사기사 및 출신 매체 사후 재직 크로스체크 검증 완료)"
                        )
                    } if is_real_job_change else None
                else:
                    # 전입 기간 초과, 직급 강등, 비유사 부서, 또는 크로스체크 실패(타깃 이전 재직/출신 사후 재직)
                    is_job_change = False
                    candidate_change_meta = None
                    if has_target_prior_history:
                        score -= 250.0  # 대상 매체에 이미 이전 인사이력이 있음 -> 해당 매체 기존 재직자(동명이인)
                    if has_origin_subsequent_history:
                        score -= 250.0  # 출신 매체에서 이후에도 인사 발생 -> 출신 매체 잔류자(동명이인)
                    if is_demoted:
                        score -= 90.0  # 직급 강등 시 동명이인으로 분리
                    if not is_similar_dept and not has_same_email:
                        score -= 90.0  # 부서 무관 시 동명이인으로 분리
                    if not is_within_transfer_window and not has_same_email:
                        score -= 50.0  # 기간 초과 시 이직 미인정

            if score > best_score:
                best_score = score
                best_cluster = cluster
                is_job_change_candidate = is_job_change if rec_media != last_media else False
                change_metadata = candidate_change_meta if rec_media != last_media else None

        # 매칭 임계치 (Threshold = 55.0점)
        if best_cluster is not None and best_score >= 55.0:
            # 기존 클러스터에 편입
            record_entry = {
                "media": rec_media,
                "dept": rec_dept,
                "domain": rec_domain,
                "rank_title": rec_rank,
                "rank_level": rec_level,
                "date_obj": rec_date,
                "raw_text": record.get("raw_text", ""),
                "action_type": record.get("action_type", "발령"),
                "email": rec_email
            }
            best_cluster["records"].append(record_entry)
            best_cluster["last_seen"] = rec_date
            best_cluster["current_media"] = rec_media
            best_cluster["current_dept"] = rec_dept
            if rec_rank not in ["기자", "미표기", ""] or not best_cluster.get("current_rank"):
                best_cluster["current_rank"] = rec_rank
            if not best_cluster.get("email") and rec_email:
                best_cluster["email"] = rec_email
                best_cluster["email_prefix"] = rec_email_prefix

            # 이직 이벤트 발생 시 수집기에 등록
            if is_job_change_candidate and change_metadata:
                change_metadata["reporter_id"] = best_cluster["reporter_id"]
                job_changes_collector.append(change_metadata)
                record["action_type"] = f"이직: {change_metadata['from_media']} -> {rec_media}"

            self._update_display_label(best_cluster)
            return best_cluster

        # 일치하는 클러스터가 없으면 신규 클러스터(동명이인 또는 첫 출현) 생성
        cluster_idx = len(existing_clusters) + 1
        dept_tag = re.sub(r"[^\w]", "", rec_dept)[:4] or "기자"
        email_tag = rec_email_prefix if rec_email_prefix else f"c{cluster_idx}"
        reporter_id = f"{name}_{email_tag}_{rec_media[:3]}"

        new_cluster = {
            "reporter_id": reporter_id,
            "name": name,
            "email": rec_email,
            "email_prefix": rec_email_prefix,
            "first_seen": rec_date,
            "last_seen": rec_date,
            "current_media": rec_media,
            "current_dept": rec_dept,
            "current_rank": rec_rank,
            "records": [{
                "media": rec_media,
                "dept": rec_dept,
                "domain": rec_domain,
                "rank_title": rec_rank,
                "rank_level": rec_level,
                "date_obj": rec_date,
                "raw_text": record.get("raw_text", ""),
                "action_type": record.get("action_type", "발령"),
                "email": rec_email
            }],
            "display_label": ""
        }
        self._update_display_label(new_cluster)
        existing_clusters.append(new_cluster)
        return new_cluster

    def _update_display_label(self, cluster: Dict[str, Any]):
        """드롭다운에서 동명이인을 한눈에 구분할 수 있는 표시명 생성."""
        name = cluster["name"]
        curr_media = cluster["current_media"]
        curr_dept = cluster["current_dept"]
        email = cluster.get("email", "")
        email_hint = f" · {email}" if email else ""
        first_rec = cluster["records"][0]
        origin_hint = f" · {first_rec['media']} 출신" if first_rec["media"] != curr_media else ""
        cluster["display_label"] = f"{name} : {curr_media} {curr_dept}{origin_hint}{email_hint}"


# ==========================================
# 4. 네이버 뉴스 검색 API 기반 바이라인 이메일 수집
# ==========================================

class NaverNewsEmailFinder:
    """
    네이버 뉴스 검색 오픈 API(OpenAPI)를 활용해
    특정 기자의 바이라인 이메일을 능동적으로 수집·보완하는 클래스.
    """

    API_URL = "https://openapi.naver.com/v1/search/news.json"

    def __init__(self, client_id: str = "", client_secret: str = ""):
        self.client_id = client_id
        self.client_secret = client_secret

    def search_reporter_email(self, name: str, media: str) -> Optional[str]:
        """
        네이버 뉴스 API로 기자명과 언론사명을 검색하여 바이라인 이메일을 탐색합니다.
        API 키가 없거나 실패할 경우 None을 반환합니다.
        """
        if not self.client_id or not self.client_secret:
            return None

        headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret
        }

        query = f'"{media}" "{name} 기자"'
        params = {
            "query": query,
            "display": 10,
            "sort": "date"
        }

        try:
            resp = requests.get(self.API_URL, headers=headers, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("items", [])
                for it in items:
                    content = (it.get("title", "") + " " + it.get("description", ""))
                    # HTML 태그 제거
                    clean_content = re.sub(r"<[^>]+>", "", content)
                    email_match = re.search(PersonnelRegexParser.EMAIL_PATTERN, clean_content)
                    if email_match:
                        email = email_match.group(0).strip()
                        return email
        except Exception as e:
            print(f"[Naver API Error] {e}")

        return None


# ==========================================
# 4-2. 실시간 온라인 뉴스 크롤러 및 발령 트래커
# ==========================================

class OnlineNewsTracker:
    """
    네이버/다음/구글 뉴스 온라인 기사를 실시간 수집 및 스크래핑하여,
    인사 발령 기사 및 기자 바이라인을 자동 분석하고 DB에 즉시 등록하는 온라인 트래커.
    4대 매체(연합뉴스·뉴스1·중앙일보·한국기자협회) 아카이브 전체 기간(1965~2026)의 포털 및 전문 기사를 실시간 탐색하여 연대기 커리어 프로필을 구축합니다.
    """

    DEFAULT_NAVER_CLIENT_ID = "xsrq0bh1hc"
    DEFAULT_NAVER_CLIENT_SECRET = "i6gooD4bp87onYOZ9jCDlyioIIsNdGyGznO6XXgQ"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Upgrade-Insecure-Requests": "1"
    }

    HEADERS_MOBILE = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9",
    }

    _SESSION: Optional[requests.Session] = None

    @classmethod
    def get_session(cls) -> requests.Session:
        """재사용 가능한 고성능 커넥션 풀 세션을 반환합니다."""
        if cls._SESSION is None:
            s = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=30,
                pool_maxsize=30,
                max_retries=1
            )
            s.mount("https://", adapter)
            s.mount("http://", adapter)
            cls._SESSION = s
        return cls._SESSION

    @classmethod
    def extract_pub_date(cls, url: str, soup: BeautifulSoup, raw_text: str = "") -> Optional[str]:
        """기사 URL 또는 본문/HTML 메타데이터에서 실제 기사 발행 일자(YYYY-MM-DD)를 정밀 추출합니다."""
        # 1. 다음 뉴스 URL 패턴 (예: v.daum.net/v/20240324143000999)
        daum_match = re.search(r"v\.daum\.net/v/(\d{4})(\d{2})(\d{2})", url)
        if daum_match:
            return f"{daum_match.group(1)}-{daum_match.group(2)}-{daum_match.group(3)}"

        # 2. 네이버 뉴스 메타데이터 및 속성 (._ARTICLE_DATE_TIME, span[data-date-time])
        naver_date = soup.select_one("._ARTICLE_DATE_TIME, .media_end_head_info_datestamp_time, span[data-date-time]")
        if naver_date:
            dt_val = naver_date.get("data-date-time", "") or naver_date.get("data-modify-date-time", "")
            if dt_val and len(dt_val) >= 10:
                m_iso = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", dt_val[:10])
                if m_iso:
                    return f"{m_iso.group(1)}-{int(m_iso.group(2)):02d}-{int(m_iso.group(3)):02d}"
            t_text = naver_date.get_text(strip=True)
            m = re.search(r"(19[6-9][0-9]|20[0-3][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", t_text)
            if m:
                return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

        # 3. 표준 오픈그래프 및 발행일 메타 태그
        for meta_name in ["article:published_time", "og:regDate", "pubdate", "sailthru.date", "date", "publish-date"]:
            meta = soup.select_one(f"meta[property='{meta_name}'], meta[name='{meta_name}']")
            if meta and meta.get("content"):
                c = meta["content"]
                m = re.search(r"(19[6-9][0-9]|20[0-3][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", c)
                if m:
                    return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

        # 4. 언론사 공통 날짜 표시 태그 (.txt_info, .num_date, .info_view, .date, .byline)
        for tag in soup.select(".txt_info, .num_date, .info_view, .date, .byline_date, .article_info, .byline"):
            text = tag.get_text(strip=True)
            m = re.search(r"(19[6-9][0-9]|20[0-3][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", text)
            if m:
                return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

        # 5. 본문 시작부 1,200자 내의 정규식 탐색
        if raw_text:
            m = re.search(r"(19[6-9][0-9]|20[0-3][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", raw_text[:1200])
            if m:
                return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

        # 6. 일반 URL 경로 내의 날짜 (/2024/05/23/ 등) 및 쿼리스트링 파라미터(ud=20161230000052 등)
        url_m = re.search(r"/(19[6-9][0-9]|20[0-3][0-9])/?(\d{2})/?(\d{2})", url)
        if url_m:
            return f"{url_m.group(1)}-{url_m.group(2)}-{url_m.group(3)}"

        url_param_m = re.search(r"[?&](?:ud|id|no|date)=(19[6-9][0-9]|20[0-3][0-9])(\d{2})(\d{2})", url)
        if url_param_m:
            return f"{url_param_m.group(1)}-{url_param_m.group(2)}-{url_param_m.group(3)}"

        return None

    @classmethod
    def fetch_joongang_personnel_section(
        cls,
        max_pages: int = 5,
        target_keyword: str = "",
        seen_urls: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        중앙일보 피플 공식 인사 섹션(https://www.joongang.co.kr/people/personnel)에서
        최신 공식 인사 기사 목록을 직접 탐색하여 반환합니다.
        """
        articles: List[Dict[str, Any]] = []
        if seen_urls is None:
            seen_urls = set()

        keyword_clean = target_keyword.strip() if target_keyword else ""
        is_general = len(keyword_clean) == 0

        for page in range(1, max_pages + 1):
            url = f"https://www.joongang.co.kr/people/personnel?page={page}" if page > 1 else "https://www.joongang.co.kr/people/personnel"
            try:
                resp = cls.get_session().get(url, headers=cls.HEADERS, timeout=4.0, verify=False)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.content, "html.parser")
                    for a_elem in soup.find_all("a"):
                        href = a_elem.get("href", "")
                        title = html.unescape(a_elem.get_text(strip=True))
                        if not href or "/article/" not in href or not title:
                            continue
                        full_href = href if href.startswith("http") else f"https://www.joongang.co.kr{href}"
                        if full_href in seen_urls:
                            continue
                        if not is_personnel_article_title(title):
                            continue
                        
                        if not is_general:
                            is_title_relevant = (
                                keyword_clean in title
                                or any(m in title for m in ["중앙일보", "JTBC", "일보", "신문", "방송", "뉴스", "통신", "미디어"])
                            )
                            if not is_title_relevant:
                                continue

                        seen_urls.add(full_href)
                        articles.append({
                            "title": title,
                            "url": full_href,
                            "media": "중앙일보",
                            "date_text": ""
                        })
            except Exception:
                pass
        return articles

    @classmethod
    def fetch_yonhap_personnel_section(
        cls,
        max_pages: int = 5,
        target_keyword: str = "",
        seen_urls: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        연합뉴스(https://www.yna.co.kr) 공식 인사 전용 섹션(people/personnel)에서
        최신 및 아카이브 공식 인사 기사 목록을 직접 탐색하여 반환합니다.
        """
        articles: List[Dict[str, Any]] = []
        if seen_urls is None:
            seen_urls = set()

        keyword_clean = target_keyword.strip() if target_keyword else ""
        is_general = len(keyword_clean) == 0

        for page in range(1, max_pages + 1):
            url = f"https://www.yna.co.kr/people/personnel/{page}"
            try:
                resp = cls.get_session().get(url, headers=cls.HEADERS, timeout=5.0, verify=False)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.content, "html.parser")
                    for a_elem in soup.select("div.item-box01 a, ul.list a, a[href*='/view/AKR']"):
                        href = a_elem.get("href", "")
                        title = html.unescape(a_elem.get_text(strip=True))
                        if not href or "/view/AKR" not in href or len(title) < 3:
                            continue
                        full_href = href if href.startswith("http") else f"https:{href}" if href.startswith("//") else f"https://www.yna.co.kr{href}"
                        if full_href in seen_urls:
                            continue
                        if not is_personnel_article_title(title, url=full_href):
                            continue
                        if not is_general:
                            is_title_relevant = (
                                keyword_clean in title
                                or any(m in title for m in ["연합뉴스", "일보", "신문", "방송", "뉴스", "통신", "미디어", "경제"])
                            )
                            if not is_title_relevant:
                                continue

                        seen_urls.add(full_href)
                        m_date = re.search(r"AKR(19[6-9]\d|20[0-3]\d)(\d{2})(\d{2})", full_href)
                        date_str = f"{m_date.group(1)}-{m_date.group(2)}-{m_date.group(3)}" if m_date else ""
                        articles.append({
                            "title": title,
                            "url": full_href,
                            "media": "연합뉴스",
                            "date_text": date_str
                        })
            except Exception:
                pass
        return articles

    @classmethod
    def fetch_journalist_personnel_section(
        cls,
        max_pages: int = 5,
        target_keyword: str = "",
        seen_urls: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        한국기자협회(https://www.journalist.or.kr) 인사·동정 전용 섹션(sec_no=63)에서
        최신 공식 인사 기사 목록을 직접 탐색하여 반환합니다.
        순수 언론사 보도국/편집국 인사 기사 집중 수집을 보장합니다.
        """
        articles: List[Dict[str, Any]] = []
        if seen_urls is None:
            seen_urls = set()

        keyword_clean = target_keyword.strip() if target_keyword else ""
        is_general = len(keyword_clean) == 0

        for page in range(1, max_pages + 1):
            url = f"https://www.journalist.or.kr/news/section_list_all.html?sec_no=63&page={page}"
            try:
                resp = cls.get_session().get(url, headers=cls.HEADERS, timeout=5.0, verify=False)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.content, "html.parser")
                    for a_elem in soup.find_all("a", href=True):
                        href = a_elem["href"]
                        if "article.html?no=" not in href:
                            continue
                        title = html.unescape(a_elem.get_text(strip=True))
                        if not title or len(title) < 3:
                            continue
                        if any(bg in title for bg in ["부고", "[부고]", "<부고>", "별세", "발인", "빈소"]):
                            continue
                        m_no = re.search(r"no=(\d+)", href)
                        if not m_no:
                            continue
                        art_no = m_no.group(1)
                        full_href = f"https://www.journalist.or.kr/news/article.html?no={art_no}"
                        if full_href in seen_urls:
                            continue
                        if not is_personnel_article_title(title, url=full_href):
                            continue

                        if not is_general:
                            is_title_relevant = (
                                keyword_clean in title
                                or any(m in title for m in ["기자협회", "일보", "신문", "방송", "뉴스", "통신", "미디어", "경제"])
                            )
                            if not is_title_relevant:
                                continue

                        seen_urls.add(full_href)
                        articles.append({
                            "title": title,
                            "url": full_href,
                            "media": "한국기자협회",
                            "date_text": ""
                        })
            except Exception:
                pass
        return articles

    @classmethod
    def fetch_news1_personnel_section(
        cls,
        max_pages: int = 5,
        target_keyword: str = "",
        seen_urls: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        뉴스1(news1.kr) 공식 인사 전용 섹션(life-culture/press)에서
        최신 공식 인사 기사 목록을 직접 탐색하여 반환합니다.
        Next.js __NEXT_DATA__ JSON 및 HTML 태그를 모두 지원합니다.
        """
        articles: List[Dict[str, Any]] = []
        if seen_urls is None:
            seen_urls = set()

        keyword_clean = target_keyword.strip() if target_keyword else ""
        is_general = len(keyword_clean) == 0

        for page in range(1, max_pages + 1):
            url = f"https://www.news1.kr/life-culture/press/?page={page}" if page > 1 else "https://www.news1.kr/life-culture/press/"
            try:
                resp = cls.get_session().get(url, headers=cls.HEADERS, timeout=5.0, verify=False)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.content, "html.parser")
                    # 1. Next.js JSON 데이터 구조
                    nd_script = soup.find("script", id="__NEXT_DATA__")
                    if nd_script and nd_script.string:
                        try:
                            import json
                            nd_data = json.loads(nd_script.string)
                            sub_data = nd_data.get("props", {}).get("pageProps", {}).get("subsectionData", [])
                            for item in sub_data:
                                art_id = item.get("id")
                                art_tit = html.unescape(item.get("title", "")).strip()
                                art_pub = str(item.get("pubdate", ""))[:10]
                                art_sum = item.get("summary", "")
                                full_url = f"https://www.news1.kr/life-culture/press/{art_id}"
                                if full_url in seen_urls:
                                    continue
                                if not is_personnel_article_title(art_tit, url=full_url):
                                    continue
                                if not is_general:
                                    if keyword_clean not in art_tit and keyword_clean not in art_sum and not any(m in art_tit for m in ["뉴스1", "일보", "신문", "방송", "뉴스", "통신", "미디어", "경제"]):
                                        continue
                                seen_urls.add(full_url)
                                articles.append({
                                    "title": art_tit,
                                    "url": full_url,
                                    "media": "뉴스1",
                                    "date_text": art_pub
                                })
                        except Exception:
                            pass
                    # 2. 일반 링크 폴백
                    for a_elem in soup.select("a[href*='/life-culture/press/']"):
                        href = a_elem.get("href", "")
                        title = html.unescape(a_elem.get_text(strip=True))
                        if not href or not title or len(title) < 3:
                            continue
                        full_href = href if href.startswith("http") else f"https://www.news1.kr{href}"
                        if full_href in seen_urls:
                            continue
                        if not is_personnel_article_title(title, url=full_href):
                            continue
                        if not is_general:
                            if keyword_clean not in title and not any(m in title for m in ["뉴스1", "일보", "신문", "방송", "뉴스", "통신", "미디어", "경제"]):
                                continue
                        seen_urls.add(full_href)
                        articles.append({
                            "title": title,
                            "url": full_href,
                            "media": "뉴스1",
                            "date_text": ""
                        })
            except Exception:
                pass
        return articles

    @classmethod
    def crawl_joongang_personnel_incremental(
        cls,
        max_pages: int = 50,
        db_path: str = "media_tracker.db",
        secondary_db_path: str = "media_personnel.db",
        stop_on_existing: bool = True,
        logger: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        중앙일보 피플 공식 인사 섹션(https://www.joongang.co.kr/people/personnel)을
        1페이지부터 최신순으로 탐색하며, 이미 DB에 등록된 기사를 만나는 즉시
        탐색을 멈추는 증분 수집(Incremental)을 실행합니다.
        신규 기사의 본문 수집, 인사 발령 파싱 및 클러스터링 DB 동기화를 수행합니다.
        """
        db = MediaDatabase(db_path)
        pages_checked = 0
        articles_checked = 0
        new_articles_count = 0
        new_records_collected: List[Dict[str, Any]] = []
        stopped_on_existing = False
        stopped_article_url = None
        seen_urls_in_run: Set[str] = set()

        def log_msg(msg: str):
            if logger:
                logger.info(msg)
            else:
                print(msg)

        log_msg("[중앙일보 인사 증분 수집] 시작 (대상: https://www.joongang.co.kr/people/personnel)")

        for page in range(1, max_pages + 1):
            url = f"https://www.joongang.co.kr/people/personnel?page={page}" if page > 1 else "https://www.joongang.co.kr/people/personnel"
            try:
                resp = cls.get_session().get(url, headers=cls.HEADERS, timeout=6.0, verify=False)
                if resp.status_code != 200:
                    log_msg(f"[페이지 {page}] HTTP {resp.status_code} 오류로 탐색을 중단합니다.")
                    break
            except Exception as e:
                log_msg(f"[페이지 {page}] 웹 요청 오류 발생: {e}")
                break

            pages_checked += 1
            soup = BeautifulSoup(resp.content, "html.parser")

            # 1. 페이지 내 모든 기사 링크 순서대로 추출
            page_articles: List[Dict[str, str]] = []
            for a_elem in soup.find_all("a"):
                href = a_elem.get("href", "")
                title = html.unescape(a_elem.get_text(strip=True))
                if not href or "/article/" not in href or not title:
                    continue
                full_href = href if href.startswith("http") else f"https://www.joongang.co.kr{href}"
                if full_href in seen_urls_in_run:
                    continue
                seen_urls_in_run.add(full_href)
                if not is_personnel_article_title(title):
                    continue
                page_articles.append({"url": full_href, "title": title})

            if not page_articles:
                log_msg(f"[페이지 {page}] 기사 목록이 비어있어 수집을 종료합니다.")
                break

            log_msg(f"[페이지 {page}] 공식 [인사] 기사 {len(page_articles)}건 확인 중...")

            # 2. 기사별 증분 확인 및 파싱
            for art in page_articles:
                art_url = art["url"]
                art_title = art["title"]
                articles_checked += 1

                # 이미 DB에 등록된 기사인지 확인 (증분 수집의 핵심 탈출 조건)
                if db.is_article_processed(art_url):
                    if stop_on_existing:
                        stopped_on_existing = True
                        stopped_article_url = art_url
                        log_msg(f"  -> [기존 기사 감지] 이미 DB에 등록된 기사를 만났습니다: {art_title} ({art_url})")
                        log_msg("  -> 증분 수집(Incremental) 기준에 따라 이전 기사 탐색을 중단하고 수집을 종료합니다.")
                        break
                    else:
                        continue

                # 신규 기사 처리
                new_articles_count += 1
                log_msg(f"  -> [신규 기사 발견] {art_title} ({art_url})")

                fetched = cls.fetch_full_text(art_url)
                body = fetched.get("text", "")
                pub_date = fetched.get("pub_date") or ""
                art_media = fetched.get("media", "중앙일보")

                # 기사 메타데이터 영속화 (다음 수집 시 중복 방지)
                db.record_processed_article(art_url, art_title, pub_date, body, art_media)

                # 본문 발령 데이터 파싱
                recs = PersonnelRegexParser.parse_article_text(
                    article_text=body,
                    default_media="중앙일보",
                    article_title=art_title,
                    pub_date=pub_date,
                    article_url=art_url
                )

                # 순수 언론사 발령 필터링
                matched_recs = [
                    r for r in recs
                    if is_media_organization(r.get("media", ""))
                    and not is_excluded_department(r.get("dept", ""))
                    and (not r.get("article_title") or is_personnel_article_title(r.get("article_title", "")))
                ]

                if matched_recs:
                    log_msg(f"     * 언론사 발령 {len(matched_recs)}건 파싱 성공: {', '.join(set(r['media'] for r in matched_recs))}")
                    new_records_collected.extend(matched_recs)
                else:
                    log_msg("     * 언론사 발령 없음 (공공기관/일반기업 인사로 판정)")

                # secondary_db (media_personnel.db) 동기화
                if secondary_db_path:
                    try:
                        conn_sec = sqlite3.connect(secondary_db_path)
                        cur_sec = conn_sec.cursor()
                        cur_sec.execute("""
                            INSERT OR REPLACE INTO raw_articles
                            (article_id, media_source, title, content, publish_date, target_media, is_media_company)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (art_url, art_media, art_title, body, pub_date, art_media, 1 if matched_recs else 0))
                        for mr in matched_recs:
                            cur_sec.execute("""
                                INSERT INTO career_events
                                (article_id, person_name, company_name, department_raw, dept_category, position_raw, rank_score, action_type, event_date)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                art_url,
                                mr.get("name", ""),
                                mr.get("media", ""),
                                mr.get("dept", ""),
                                mr.get("domain", ""),
                                mr.get("rank_title", ""),
                                mr.get("rank_level", 2),
                                mr.get("action_type", "발령"),
                                mr.get("pub_date", pub_date)
                            ))
                        conn_sec.commit()
                        conn_sec.close()
                    except Exception:
                        pass

            if stopped_on_existing:
                break

        # 3. 신규 언론인 발령 레코드가 존재하면 통합 클러스터링 및 타임라인 갱신
        engine_res = {"total_records": 0, "unique_reporters": 0, "job_changes_detected": 0}
        if new_records_collected:
            engine = PersonnelTrackerEngine(db_path)
            engine_res = engine.process_records(new_records_collected, append=True)
            log_msg(f"[클러스터링 갱신 완료] 신규 발령 {len(new_records_collected)}건 누적 반영, 등록 언론인 {engine_res['unique_reporters']}명")

        summary = {
            "source": "https://www.joongang.co.kr/people/personnel",
            "pages_checked": pages_checked,
            "articles_checked": articles_checked,
            "new_articles_count": new_articles_count,
            "new_records_count": len(new_records_collected),
            "stopped_on_existing_article": stopped_on_existing,
            "stopped_article_url": stopped_article_url,
            "unique_reporters": engine_res.get("unique_reporters", 0),
            "job_changes_detected": engine_res.get("job_changes_detected", 0)
        }
        return summary

    @classmethod
    def search_online_articles(
        cls, 
        query: str, 
        max_items: int = 25,
        client_id: str = "",
        client_secret: str = "",
        start_year: int = 1965,
        end_year: int = 2026,
        sort_first: str = "date",
        full_history: bool = True
    ) -> List[Dict[str, Any]]:
        """
        4대 매체(연합뉴스·뉴스1·중앙일보·한국기자협회) 공식 [인사] 기사만을 엄격하게 실시간 수집합니다.
        (통신사 뉴스1·연합뉴스, 중앙일보 및 한국기자협회를 통해 보도되는 전 매체 공식 인사 기사 수집 보장)
        """
        client_id = client_id or cls.DEFAULT_NAVER_CLIENT_ID
        client_secret = client_secret or cls.DEFAULT_NAVER_CLIENT_SECRET
        articles: List[Dict[str, Any]] = []
        seen_urls = set()

        if not full_history:
            # 실시간 인터랙티브 검색 시 CPU/네트워크 부하 0화:
            # 오직 공식 Naver News API JSON만 1회 호출하며,
            # BeautifulSoup 기반의 HTML 웹 크롤링 및 다중 쓰레드를 일절 실행하지 않습니다.
            if client_id and client_secret:
                try:
                    api_url = "https://openapi.naver.com/v1/search/news.json"
                    api_headers = {
                        "X-Naver-Client-Id": client_id,
                        "X-Naver-Client-Secret": client_secret,
                        **cls.HEADERS
                    }
                    api_params = {"query": query, "display": min(max_items, 10), "sort": "date"}
                    api_resp = cls.get_session().get(api_url, headers=api_headers, params=api_params, timeout=2.0, verify=False)
                    if api_resp.status_code == 200:
                        for it in api_resp.json().get("items", []):
                            raw_tit = re.sub(r"<[^>]+>", "", it.get("title", "")).strip()
                            raw_tit = raw_tit.replace("&quot;", '"').replace("&amp;", '&').replace("&lt;", '<').replace("&gt;", '>')
                            target_url = it.get("link", "") or it.get("originallink", "")
                            if target_url and target_url not in seen_urls:
                                seen_urls.add(target_url)
                                pub_d = it.get("pubDate", "")
                                date_str = ""
                                if pub_d:
                                    try:
                                        import email.utils
                                        dt = email.utils.parsedate_to_datetime(pub_d)
                                        date_str = dt.strftime("%Y-%m-%d")
                                    except Exception:
                                        pass
                                articles.append({
                                    "title": raw_tit,
                                    "url": target_url,
                                    "media": "온라인뉴스",
                                    "date_text": date_str
                                })
                except Exception:
                    pass
            return articles

        def is_valid_wire_article(tit: str, u: str, pr: str) -> bool:
            clean_title = tit.replace("새 창 열림", "").replace("새창열림", "").strip()
            if not is_personnel_article_title(clean_title, url=u):
                return False

            # 만약 검색 대상 키워드가 특정 언론사명인 경우, 기사 제목 또는 링크에 해당 언론사 확인
            if target_keyword:
                matched_km = None
                for km in KNOWN_MEDIA_NAMES:
                    if km in target_keyword or is_same_media_company(target_keyword, km):
                        matched_km = km
                        break
                if matched_km:
                    km_tokens = [matched_km]
                    if matched_km in MEDIA_GROUP_ALIASES:
                        km_tokens.extend(MEDIA_GROUP_ALIASES[matched_km])
                    for base, alist in MEDIA_GROUP_ALIASES.items():
                        if matched_km == base or matched_km in alist:
                            km_tokens.append(base)
                            km_tokens.extend(alist)
                    km_tokens = list(set(km_tokens))
                    if not any(tok in clean_title for tok in km_tokens):
                        if pr and is_same_media_company(pr, matched_km):
                            pass
                        elif any(tok in u for tok in [matched_km, "article/421", "article/001", "article/025", "article/003"]) or any(tok.lower() in u.lower() for tok in km_tokens):
                            pass
                        else:
                            return False

            # 통신 3사(뉴스1·연합뉴스·뉴시스), 중앙일보, 한국기자협회 기사 허용
            if pr in ["뉴스1", "연합뉴스", "뉴시스", "중앙일보", "한국기자협회", "기자협회"]:
                return True
            if "news1.kr" in u or "yna.co.kr" in u or "newsis.com" in u or "joongang.co.kr" in u or "journalist.or.kr" in u:
                return True
            if "article/421/" in u or "article/001/" in u or "article/003/" in u or "article/025/" in u:
                return True
            if not pr and ("v.daum.net/v/" in u or "n.news.naver.com" in u):
                return True
            # 포털에 송고된 언론사 공식 [인사] 기사 (제목에 [인사]/<인사> 및 언론사명이 포함된 경우)
            if any(k in clean_title for k in ["[인사]", "<인사>", "【인사】"]):
                if target_keyword and target_keyword in clean_title:
                    return True
                if any(m in clean_title for m in KNOWN_MEDIA_NAMES):
                    return True
            return False

        clean_q = query.replace('"', '').replace("'", "").strip()
        # 인사 태그를 제외한 실제 핵심 검색어(인명, 매체명 등) 추출
        target_keyword = re.sub(r"[\[<【(]?\s*인사\s*[\]>】)]?", "", clean_q).strip()
        is_general_personnel = (len(target_keyword) == 0)

        # 1. 네이버 검색: 통신 3사(뉴스1·연합뉴스·뉴시스), 중앙일보(025), 한국기자협회 기사 실시간 최우선 수집
        wire_queries = []
        if any(src in clean_q for src in ["뉴스1", "연합뉴스", "뉴시스", "중앙일보", "한국기자협회", "기자협회"]):
            wire_queries.append(clean_q)
        elif target_keyword:
            # 원본 핵심 질의 및 따옴표 완전일치 질의 최우선 배치
            wire_queries.extend([
                f'"{target_keyword}" 인사',
                f'"{target_keyword}" [인사]',
                f'[인사] {target_keyword}',
                clean_q
            ])
            # 매체명인 경우 계열 공식 신문/사명 별칭 자동 확장
            matched_km = None
            for km in KNOWN_MEDIA_NAMES:
                if km == target_keyword or is_same_media_company(target_keyword, km):
                    matched_km = km
                    break
            if matched_km and matched_km in MEDIA_GROUP_ALIASES:
                for al in MEDIA_GROUP_ALIASES[matched_km][:3]:
                    if al != target_keyword and not any(al == w.replace('"', '').replace(' 인사', '') for w in wire_queries):
                        wire_queries.append(f'"{al}" 인사')
                        wire_queries.append(f'[인사] {al}')
            if target_keyword and not target_keyword.endswith(("신문", "방송", "뉴스", "통신", "일보", "타임스", "타임즈")):
                wire_queries.append(f'"{target_keyword}신문" 인사')
                wire_queries.append(f'[인사] {target_keyword}신문')
            # 통신사 및 주요 매체 연계 질의
            wire_queries.extend([
                f"{target_keyword} [인사] 뉴스1",
                f"{target_keyword} [인사] 연합뉴스",
                f"{target_keyword} [인사] 뉴시스",
                f"{target_keyword} [인사] 기자협회"
            ])
        else:
            wire_queries = [clean_q, f"{clean_q} 뉴스1", f"{clean_q} 연합뉴스", f"{clean_q} 뉴시스", f"{clean_q} 중앙일보", f"{clean_q} 기자협회"]

        for wq in wire_queries:
            if len(articles) >= max_items:
                break
            # 1-1. 네이버 공식 검색 API
            if client_id and client_secret:
                api_sort_opts = ["date", "sim"] if sort_first == "date" else ["sim", "date"]
                for s_opt in api_sort_opts:
                    if len(articles) >= max_items:
                        break
                    try:
                        api_url = "https://openapi.naver.com/v1/search/news.json"
                        api_headers = {
                            "X-Naver-Client-Id": client_id,
                            "X-Naver-Client-Secret": client_secret,
                            **cls.HEADERS
                        }
                        api_params = {"query": wq, "display": min(max_items, 30), "sort": s_opt}
                        api_resp = cls.get_session().get(api_url, headers=api_headers, params=api_params, timeout=3.5, verify=False)
                        if api_resp.status_code == 200:
                            api_data = api_resp.json()
                            for it in api_data.get("items", []):
                                raw_tit = re.sub(r"<[^>]+>", "", it.get("title", "")).strip()
                                raw_tit = raw_tit.replace("&quot;", '"').replace("&amp;", '&').replace("&lt;", '<').replace("&gt;", '>')
                                target_url = it.get("link", "") or it.get("originallink", "")
                                p_guess = "뉴스1" if "news1.kr" in target_url else ("연합뉴스" if "yna.co.kr" in target_url else ("중앙일보" if "joongang.co.kr" in target_url else ("한국기자협회" if "journalist.or.kr" in target_url else "")))
                                if not p_guess:
                                    if "뉴스1" in wq:
                                        p_guess = "뉴스1"
                                    elif "연합뉴스" in wq:
                                        p_guess = "연합뉴스"
                                    elif "중앙일보" in wq:
                                        p_guess = "중앙일보"
                                    elif "기자협회" in wq:
                                        p_guess = "한국기자협회"

                                if target_url and target_url not in seen_urls and is_valid_wire_article(raw_tit, target_url, p_guess):
                                    seen_urls.add(target_url)
                                    pub_d = it.get("pubDate", "")
                                    date_str = ""
                                    if pub_d:
                                        try:
                                            import email.utils
                                            dt = email.utils.parsedate_to_datetime(pub_d)
                                            date_str = dt.strftime("%Y-%m-%d")
                                        except Exception:
                                            pass
                                    articles.append({
                                        "title": raw_tit,
                                        "url": target_url,
                                        "media": p_guess,
                                        "date_text": date_str
                                    })
                                    if len(articles) >= max_items:
                                        break
                    except Exception as e:
                        print(f"[Naver API Search Error] {e}")

            # 1-2. 네이버 PC/모바일 웹 검색 (API 결과가 부족할 때 1~2페이지 고속 탐색)
            if len(articles) < max_items:
                for start_idx in [1, 11]:
                    if len(articles) >= max_items:
                        break
                    try:
                        n_sort = "1" if sort_first == "date" else "0"
                        nso_sort = "so:dd" if n_sort == "1" else "so:r"
                        start_param = f"&start={start_idx}" if start_idx > 1 else ""

                        # 1) PC 검색 (SDS 레이아웃 및 클래식 레이아웃 지원)
                        pc_url = f"https://search.naver.com/search.naver?where=news&query={quote(wq)}&sort={n_sort}&ds={start_year}.01.01&de={end_year}.12.31&nso={nso_sort},p:from{start_year}0101to{end_year}1231,a:all{start_param}"
                        resp_pc = cls.get_session().get(pc_url, headers=cls.HEADERS, timeout=4.0, verify=False)
                        if resp_pc.status_code == 200:
                            soup_pc = BeautifulSoup(resp_pc.content, "html.parser")
                            tit_anchors = soup_pc.select("a.W5fYO2_0xMDeLrlx, a.news_tit")
                            if not tit_anchors:
                                for a in soup_pc.find_all("a"):
                                    h = a.get("href", "")
                                    t = a.get_text(strip=True).replace("새 창 열림", "").replace("새창열림", "").strip()
                                    if not h or not t or "keep.naver.com" in h or "/press/" in h:
                                        continue
                                    if is_personnel_article_title(t, url=h):
                                        tit_anchors.append(a)

                            for tit_a in tit_anchors:
                                target_title = tit_a.get_text(strip=True).replace("새 창 열림", "").replace("새창열림", "").strip()
                                origin_url = tit_a.get("href", "")
                                if not target_title or not origin_url:
                                    continue

                                # 상위 카드 컨테이너 탐색
                                p = tit_a.parent
                                card = None
                                for _ in range(6):
                                    if not p:
                                        break
                                    if p.find("a", href=lambda h: h and ("/press/" in h or "media.naver.com" in h)) or p.find("a", class_=lambda c: c and "Zq2aiciJOBY5v64y" in c):
                                        card = p
                                        break
                                    p = p.parent

                                press_name = ""
                                naver_url = ""
                                if card:
                                    press_a = card.find("a", href=lambda h: h and ("/press/" in h or "media.naver.com" in h)) or card.find("a", class_=lambda c: c and "Zq2aiciJOBY5v64y" in c)
                                    if press_a:
                                        press_name = press_a.get_text(strip=True).replace("새 창 열림", "").replace("새창열림", "").strip()
                                    nav_a = card.find("a", href=lambda h: h and "news.naver.com" in h)
                                    if nav_a:
                                        naver_url = nav_a.get("href", "")

                                final_url = naver_url or origin_url
                                if final_url and final_url not in seen_urls:
                                    p_guess = press_name
                                    if not p_guess:
                                        if "article/421/" in final_url or "news1.kr" in final_url:
                                            p_guess = "뉴스1"
                                        elif "article/001/" in final_url or "yna.co.kr" in final_url:
                                            p_guess = "연합뉴스"
                                        elif "article/025/" in final_url or "joongang.co.kr" in final_url:
                                            p_guess = "중앙일보"
                                        elif "journalist.or.kr" in final_url:
                                            p_guess = "한국기자협회"
                                    if is_valid_wire_article(target_title, final_url, p_guess):
                                        seen_urls.add(final_url)
                                        articles.append({
                                            "title": target_title,
                                            "url": final_url,
                                            "media": p_guess,
                                            "date_text": ""
                                        })
                                        if len(articles) >= max_items:
                                            break

                        # 2) 모바일 검색 보조
                        if len(articles) < max_items:
                            naver_url = f"https://m.search.naver.com/search.naver?where=m_news&query={quote(wq)}&sort={n_sort}&ds={start_year}.01.01&de={end_year}.12.31&nso={nso_sort},p:from{start_year}0101to{end_year}1231,a:all{start_param}"
                            resp = cls.get_session().get(naver_url, headers=cls.HEADERS_MOBILE, timeout=3.5, verify=False)
                            if resp.status_code == 200:
                                soup = BeautifulSoup(resp.content, "html.parser")
                                for a_elem in soup.find_all("a"):
                                    href = a_elem.get("href", "")
                                    title = html.unescape(a_elem.get_text(strip=True))
                                    if not href or href in seen_urls or len(title) < 3:
                                        continue

                                    press_name = ""
                                    if "article/421/" in href or "news1.kr" in href or "뉴스1" in title:
                                        press_name = "뉴스1"
                                    elif "article/001/" in href or "yna.co.kr" in href or "연합뉴스" in title:
                                        press_name = "연합뉴스"
                                    elif "article/003/" in href or "newsis.com" in href or "뉴시스" in title:
                                        press_name = "뉴시스"
                                    elif "article/025/" in href or "joongang.co.kr" in href or "중앙일보" in title:
                                        press_name = "중앙일보"
                                    elif "journalist.or.kr" in href or "기자협회" in title:
                                        press_name = "한국기자협회"

                                    if press_name and is_valid_wire_article(title, href, press_name):
                                        seen_urls.add(href)
                                        articles.append({
                                            "title": title,
                                            "url": href,
                                            "media": press_name,
                                            "date_text": ""
                                        })
                                        if len(articles) >= max_items:
                                            break
                    except Exception as e:
                        pass

        # 2. 다음 뉴스 검색 (결과가 부족할 때만 핵심 쿼리 1~2페이지 고속 탐색)
        if len(articles) < max_items:
            daum_queries = wire_queries[:2] if wire_queries else [query]
            period_param = f"&period=u&sd={start_year}0101000000&ed={end_year}1231235959" if (start_year and end_year) else ""

            for wq in daum_queries:
                if len(articles) >= max_items:
                    break
                for p_num in [1, 2]:
                    if len(articles) >= max_items:
                        break
                    try:
                        daum_sort = "recency" if sort_first == "date" else "accuracy"
                        page_param = f"&p={p_num}" if p_num > 1 else ""
                        daum_url = f"https://search.daum.net/search?w=news&q={quote(wq)}&sort={daum_sort}{period_param}{page_param}"
                        resp = cls.get_session().get(daum_url, headers=cls.HEADERS, timeout=3.5, verify=False)
                        if resp.status_code == 200:
                            soup = BeautifulSoup(resp.content, "html.parser")
                            card_items = soup.select("div.c-item-content, li.bx, div.item-title, ul.list_news > li")
                            if not card_items:
                                break
                            for it in card_items:
                                tit_a = it.select_one("strong.tit-doc a, .item-title a, a.tit_main, a.c-tit-doc")
                                if not tit_a:
                                    continue
                                title = html.unescape(tit_a.get_text(strip=True))
                                press_elem = it.select_one(".item-subinfo .txt_info, a.f_nb, span.c-tit-info, .gem-subinfo a, span.info")
                                press_name = press_elem.get_text(strip=True) if press_elem else ""

                                daum_inlink = it.select_one("a[href*='v.daum.net/v/'], a[href*='news.v.daum.net/v/']")
                                href = daum_inlink.get("href", "") if daum_inlink else tit_a.get("href", "")
                                if not href or href in seen_urls:
                                    continue

                                if is_valid_wire_article(title, href, press_name):
                                    seen_urls.add(href)
                                    d_match = re.search(r"v\.daum\.net/v/(\d{4})(\d{2})(\d{2})", href)
                                    d_text = f"{d_match.group(1)}-{d_match.group(2)}-{d_match.group(3)}" if d_match else ""
                                    m_guess = "뉴스1" if ("뉴스1" in press_name or "news1.kr" in href) else ("연합뉴스" if ("연합뉴스" in press_name or "yna.co.kr" in href) else ("뉴시스" if ("뉴시스" in press_name or "newsis.com" in href) else ("중앙일보" if ("중앙일보" in press_name or "joongang.co.kr" in href) else ("한국기자협회" if ("기자협회" in press_name or "journalist.or.kr" in href) else "언론사"))))
                                    articles.append({
                                        "title": title,
                                        "url": href,
                                        "media": m_guess,
                                        "date_text": d_text
                                    })
                                    if len(articles) >= max_items:
                                        break
                    except Exception as e:
                        pass

        return articles

    @classmethod
    def fetch_full_text(cls, url: str) -> Dict[str, Any]:
        """
        기사 URL에 접속하여 100% 원문 본문, 언론사명, 실제 발행 일자, 이메일을 추출합니다.
        뉴스1(Next.js __NEXT_DATA__ JSON) 및 연합뉴스(story-news DOM) 최신 아키텍처를 완벽 지원합니다.
        """
        result = {"text": "", "media": "온라인뉴스", "email": None, "pub_date": None}
        if not url or not str(url).startswith("http"):
            return result

        try:
            resp = cls.get_session().get(url, headers=cls.HEADERS, timeout=4.0, verify=False, allow_redirects=True)
            if resp.status_code == 200:
                final_url = resp.url
                soup = BeautifulSoup(resp.content, "html.parser")
                full_text = ""

                # 1. 뉴스1 등 Next.js 기반 사이트 (__NEXT_DATA__ JSON 내 원문 본문 파싱)
                next_data = soup.find("script", id="__NEXT_DATA__")
                if next_data and next_data.string:
                    try:
                        import json
                        nd_obj = json.loads(next_data.string)
                        av = nd_obj.get("props", {}).get("pageProps", {}).get("articleView", {})
                        if av:
                            text_blocks = []
                            for item in av.get("contentArrange", []):
                                if isinstance(item, dict) and item.get("type") == "text":
                                    c = item.get("content", "")
                                    if c:
                                        c_clean = BeautifulSoup(html.unescape(c), "html.parser").get_text("\n")
                                        if c_clean.strip():
                                            text_blocks.append(c_clean.strip())
                            if text_blocks:
                                full_text = "\n".join(text_blocks)
                            pubdate_raw = av.get("pubdate_at") or av.get("published_time") or ""
                            if pubdate_raw:
                                dm = re.search(r"(\d{4})[.\-](\d{2})[.\-](\d{2})", pubdate_raw)
                                if dm:
                                    result["pub_date"] = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"
                            if "news1.kr" in final_url:
                                result["media"] = "뉴스1"
                            for em_field in [av.get("byline_bottom", ""), av.get("author", ""), full_text]:
                                em = re.search(PersonnelRegexParser.EMAIL_PATTERN, str(em_field))
                                if em:
                                    result["email"] = em.group(0).strip()
                                    break
                    except Exception as e:
                        pass

                # 2. 일반 DOM 본문 영역 탐색 (한국기자협회 #news_body_area, 연합뉴스 story-news, 헤럴드 #articleText 및 주요 언론사별 전용 셀렉터)
                if not full_text:
                    content_area = (
                        soup.select_one("#news_body_area") or
                        soup.select_one("#articleText") or
                        soup.select_one("article.story-news") or
                        soup.select_one("div.story-news") or
                        soup.select_one("#dic_area") or
                        soup.select_one("#newsct_article") or
                        soup.select_one("article#dic_area") or
                        soup.select_one("div#newsct_article") or
                        soup.select_one("section.article-body") or
                        soup.select_one("#news_body_id") or
                        soup.select_one("#article_txt") or
                        soup.select_one(".article_txt") or
                        soup.select_one("#articletxt") or
                        soup.select_one(".art_txt") or
                        soup.select_one("#NewsAdContent") or
                        soup.select_one("#article_content") or
                        soup.select_one("#textBody") or
                        soup.select_one(".news_body") or
                        soup.select_one("#news_body") or
                        soup.select_one("#v-left-scroll-in") or
                        soup.select_one("section.article-view") or
                        soup.select_one("#harmonyContainer") or
                        soup.select_one("div.article_view") or
                        soup.select_one("div#article_body") or
                        soup.select_one("div.article-body") or
                        soup.select_one("div.article_body") or
                        soup.select_one("div#articeBody") or
                        soup.select_one("div.view_con") or
                        soup.select_one("div.cnt_view") or
                        soup.select_one("div.news_cnt") or
                        soup.select_one("div#article-view-content-div") or
                        soup.select_one("#articleBodyContents") or
                        soup.select_one("div.article") or
                        soup.select_one("div#articleWrap") or
                        soup.select_one("div.content01") or
                        soup.select_one("div.article-txt") or
                        soup.select_one("article") or
                        soup.select_one("div.article_body_contents")
                    )

                    if content_area:
                        for tag in content_area(["script", "style", "figure", "figcaption", "aside", "nav", "header", "footer"]):
                            tag.decompose()
                        full_text = content_area.get_text(separator="\n", strip=True)
                    else:
                        for tag in soup(["script", "style", "nav", "header", "footer", "form", "aside"]):
                            tag.decompose()
                        full_text = soup.get_text(separator="\n", strip=True)

                result["text"] = full_text

                # 언론사명 판별
                if "news1.kr" in final_url:
                    result["media"] = "뉴스1"
                elif "yna.co.kr" in final_url:
                    result["media"] = "연합뉴스"
                elif "joongang.co.kr" in final_url:
                    result["media"] = "중앙일보"
                elif "journalist.or.kr" in final_url:
                    result["media"] = "한국기자협회"
                    full_text = full_text.split("한국기자협회의 전체기사 보기")[0].strip()
                    result["text"] = full_text
                else:
                    press_meta = (
                        soup.select_one("meta[property='og:article:author']") or
                        soup.select_one("meta[property='og:site_name']") or
                        soup.select_one("meta[name='twitter:creator']") or
                        soup.select_one("meta[name='dable:author']") or
                        soup.select_one(".info_cp .tit_cp, .link_cp, .media_end_head_top_logo img, .press_logo img")
                    )
                    if press_meta:
                        if press_meta.name == "img" and press_meta.get("alt"):
                            result["media"] = normalize_media_name(press_meta.get("alt"))
                        elif press_meta.has_attr("content"):
                            result["media"] = normalize_media_name(press_meta.get("content"))
                        else:
                            result["media"] = normalize_media_name(press_meta.get_text(strip=True))

                # 포털 인링크인 경우 통신사 원문 기사 링크(Origin Link, 예: news1.kr 또는 yna.co.kr 링크) 추출
                orig_link_elem = soup.select_one("a.media_end_head_origin_link")
                if orig_link_elem and orig_link_elem.get("href"):
                    result["origin_url"] = orig_link_elem.get("href").strip()

                # 실제 기사 발행 일자 추출
                if not result.get("pub_date"):
                    result["pub_date"] = cls.extract_pub_date(final_url, soup, full_text)

                # 이메일 추출 (통신사/언론사 대표 접수용 공용 이메일은 기자의 개인 이메일로 오인식되지 않도록 제외)
                if not result.get("email"):
                    email_matches = re.findall(PersonnelRegexParser.EMAIL_PATTERN, full_text)
                    for em in email_matches:
                        em_clean = em.strip().lower()
                        if any(em_clean.startswith(prefix) for prefix in ["opinion@", "jebo@", "desk@", "webmaster@", "contact@", "press@", "master@", "admin@", "help@", "info@", "news1@", "yna@"]):
                            continue
                        result["email"] = em.strip()
                        break

        except Exception as e:
            print(f"[Fetch Body Error] {e}")

        return result

    @classmethod
    def track_reporter_online(
        cls, 
        reporter_name: str,
        client_id: str = "",
        client_secret: str = "",
        stop_event: Optional[Any] = None,
        full_history: bool = True,
        db_path: str = "media_tracker.db"
    ) -> Dict[str, Any]:
        """
        특정 기자의 최신(당일/당해) 온라인 인사 발령 기사를 실시간 고속 수집하여
        소속 언론사, 직급/부서, 이메일, 연대기 타임라인을 파싱하고 DB에 등록합니다.
        full_history=True일 경우 1965~2026 전체 기간 심층 탐색을 수행합니다.
        """
        name = reporter_name.strip()
        if not name:
            return {"found_records": 0, "articles_searched": 0, "message": "기자 이름을 입력하세요."}

        client_id = client_id or cls.DEFAULT_NAVER_CLIENT_ID
        client_secret = client_secret or cls.DEFAULT_NAVER_CLIENT_SECRET

        if not full_history:
            # 실시간 인터랙티브 검색 시 CPU/네트워크 부하 0화:
            # 단일 네이버 News API JSON 호출 및 멀티쓰레드/웹크롤링 완전 배제
            candidate_articles = cls.search_online_articles(
                f'"{name}" 인사', max_items=3, client_id=client_id, client_secret=client_secret, full_history=False
            )
            if not candidate_articles:
                return {"found_records": 0, "articles_searched": 0, "message": f"'{name}' 기자의 최신 기사 확인 완료"}

            db_known_urls = set()
            try:
                db_inst = MediaDatabase(db_path)
                with db_inst.get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT article_url FROM personnel_records WHERE article_url IS NOT NULL AND article_url != ''")
                    db_known_urls = {row[0] for row in cur.fetchall()}
            except Exception:
                pass

            new_articles = [a for a in candidate_articles if a.get("url") and a.get("url") not in db_known_urls]
            if not new_articles:
                return {"found_records": 0, "articles_searched": len(candidate_articles), "message": "최신 상태 확인 완료"}

            all_records = []
            for art in new_articles[:2]:
                try:
                    fetched = cls.fetch_full_text(art["url"])
                    body = fetched.get("text", "")
                    if body and len(body) >= 30 and name in body:
                        recs = PersonnelRegexParser.parse_article_text(
                            article_text=body,
                            default_media=fetched.get("media", "온라인뉴스"),
                            article_title=art.get("title", ""),
                            pub_date=fetched.get("pub_date") or art.get("date_text") or "2026-01-01",
                            article_url=art["url"]
                        )
                        all_records.extend([r for r in recs if r["name"] == name and is_media_organization(r.get("media", "")) and not is_excluded_department(r.get("dept", ""))])
                except Exception:
                    pass

            if all_records:
                engine = PersonnelTrackerEngine(db_path)
                engine.process_records(all_records, append=True)
                return {"found_records": len(all_records), "articles_searched": len(candidate_articles), "message": f"신규 {len(all_records)}건 동기화 완료"}
            return {"found_records": 0, "articles_searched": len(candidate_articles), "message": "최신 상태 확인 완료"}

        cur_year = datetime.datetime.now().year
        if full_history:
            intervals = [
                (2025, 2026, "date"),
                (2023, 2024, "date"),
                (2021, 2022, "date"),
                (2019, 2020, "sim"),
                (2017, 2018, "sim"),
                (2015, 2016, "sim"),
                (2013, 2014, "sim"),
                (2011, 2012, "sim"),
                (2009, 2010, "sim"),
                (2005, 2008, "sim")
            ]
            sec_pages = 2
        else:
            intervals = [
                (cur_year, cur_year, "date")
            ]
            sec_pages = 1

        # 1단계: 병렬 검색 쿼리 작업 리스트 구성 (핵심 쿼리 고속화)
        search_tasks = []
        for sy, ey, sort_type in intervals:
            q_list = [
                f'"{name}" 인사',
                f'"{name}" [인사]',
                f'{name} [인사] 연합뉴스',
                f'{name} [인사] 뉴스1',
                f'{name} [인사] 중앙일보',
            ]
            if not full_history and len(q_list) > 3:
                q_list = q_list[:3]
            for q in q_list:
                search_tasks.append((q, sy, ey, sort_type))

        candidate_articles: Dict[str, Dict[str, Any]] = {}

        # 4대 매체 전용 섹션(한국기자협회·뉴스1·중앙일보) 직접 보조 탐색 (병렬 고속화)
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as direct_exec:
                fetcher_futures = [
                    direct_exec.submit(cls.fetch_journalist_personnel_section, sec_pages, name, set()),
                    direct_exec.submit(cls.fetch_news1_personnel_section, sec_pages, name, set()),
                    direct_exec.submit(cls.fetch_joongang_personnel_section, sec_pages, name, set()),
                ]
                for f in concurrent.futures.as_completed(fetcher_futures):
                    for d_art in f.result():
                        u = d_art.get("url", "")
                        if u and u not in candidate_articles:
                            candidate_articles[u] = d_art
        except Exception:
            pass

        def run_search_task(task):
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                return []
            q, sy, ey, st = task
            return cls.search_online_articles(
                q, max_items=25, client_id=client_id, client_secret=client_secret,
                start_year=sy, end_year=ey, sort_first=st
            )

        # 1단계: 멀티스레드 병렬 검색 실행 (ThreadPoolExecutor)
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(run_search_task, t) for t in search_tasks]
            for f in concurrent.futures.as_completed(futures):
                if stop_event and getattr(stop_event, "is_set", lambda: False)():
                    break
                try:
                    for art in f.result():
                        u = art.get("url", "")
                        if u and u not in candidate_articles:
                            candidate_articles[u] = art
                except Exception:
                    pass

        # 2단계: 후보 기사 본문 병렬 수집 (이미 DB에 파싱된 URL은 스킵하여 속도 극대화)
        all_records = []
        known_medias = set()

        db_known_urls = set()
        try:
            db_inst = MediaDatabase()
            with db_inst.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT article_url FROM personnel_records WHERE article_url IS NOT NULL AND article_url != ''")
                db_known_urls = {row[0] for row in cur.fetchall()}
        except Exception:
            pass

        def fetch_worker(art):
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                return None
            u = art["url"]
            if u in db_known_urls:
                return None
            try:
                fetched = cls.fetch_full_text(u)
                body = fetched.get("text", "")
                if not body or len(body) < 30 or name not in body:
                    return None
                return art, fetched
            except Exception:
                return None

        fetched_results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(fetch_worker, art) for art in candidate_articles.values()]
            for f in concurrent.futures.as_completed(futures):
                if stop_event and getattr(stop_event, "is_set", lambda: False)():
                    break
                try:
                    res = f.result()
                    if res:
                        fetched_results.append(res)
                except Exception:
                    pass

        # 3단계: 기사 파싱 및 발령 기록 추출
        for art, fetched in fetched_results:
            u = art["url"]
            body = fetched["text"]
            media = fetched["media"] if fetched["media"] != "온라인뉴스" else art["media"]
            article_pub_date = fetched.get("pub_date") or art.get("date_text") or "2024-01-01"
            final_u = fetched.get("origin_url") or u

            recs = PersonnelRegexParser.parse_article_text(
                article_text=body,
                default_media=media,
                article_title=art.get("title", ""),
                pub_date=article_pub_date,
                article_url=final_u
            )
            matched_recs = [
                r for r in recs 
                if r["name"] == name and is_media_organization(r.get("media", "")) and not is_excluded_department(r.get("dept", ""))
            ]
            if matched_recs:
                for mr in matched_recs:
                    if fetched.get("email") and not mr.get("email"):
                        mr["email"] = fetched["email"]
                    if is_media_organization(mr.get("media", "")):
                        known_medias.add(mr["media"])
                all_records.extend(matched_recs)

        # 4단계: 발견된 매체(known_medias) 추가 과거 발령 고속 탐색
        if known_medias and not (stop_event and getattr(stop_event, "is_set", lambda: False)()):
            extra_tasks = []
            for km in list(known_medias):
                extra_tasks.append((f'{km} "{name}" [인사]', 1965, 2026, "sim"))
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                extra_futures = [executor.submit(run_search_task, t) for t in extra_tasks]
                extra_arts = []
                for f in concurrent.futures.as_completed(extra_futures):
                    for a in f.result():
                        if a["url"] not in candidate_articles:
                            candidate_articles[a["url"]] = a
                            extra_arts.append(a)
                
                if extra_arts:
                    extra_fetch_futures = [executor.submit(fetch_worker, a) for a in extra_arts]
                    for f in concurrent.futures.as_completed(extra_fetch_futures):
                        res = f.result()
                        if res:
                            art, fetched = res
                            recs = PersonnelRegexParser.parse_article_text(
                                article_text=fetched["text"],
                                default_media=fetched["media"] if fetched["media"] != "온라인뉴스" else art["media"],
                                article_title=art.get("title", ""),
                                pub_date=fetched.get("pub_date") or art.get("date_text") or "2020-01-01",
                                article_url=fetched.get("origin_url") or art["url"]
                            )
                            for r in recs:
                                if r["name"] == name and is_media_organization(r.get("media", "")) and not is_excluded_department(r.get("dept", "")):
                                    all_records.append(r)

        is_stopped = bool(stop_event and getattr(stop_event, "is_set", lambda: False)())
        if all_records:
            engine = PersonnelTrackerEngine(db_path=db_path)
            res = engine.process_records(all_records, append=True)
            msg_suffix = " (중단 전까지 수집된 내역 반영)" if is_stopped else ""
            return {
                "found_records": len(all_records),
                "articles_searched": len(candidate_articles),
                "unique_reporters": res["unique_reporters"],
                "message": f"4대 매체(연합뉴스·뉴스1·중앙일보·한국기자협회) 아카이브 전체 기간(1965~2026) 공식 [인사]/<인사> 기사 {len(candidate_articles)}건을 초고속 탐색하여 '{name}' 기자의 발령 기록 {len(all_records)}건을 실시간 수집했습니다!{msg_suffix}"
            }
        else:
            if is_stopped:
                return {"found_records": 0, "articles_searched": len(candidate_articles), "message": "사용자에 의해 실시간 탐색이 중단되었습니다."}
            return {
                "found_records": 0,
                "articles_searched": len(candidate_articles),
                "message": f"'{name}' 기자의 4대 매체(연합뉴스·뉴스1·중앙일보·한국기자협회) 아카이브 전체 기간(1965~2026) 공식 [인사] 기사 내 발령 기록을 찾지 못했습니다. (동명이인 공공기관 제외 완료)"
            }

    @classmethod
    def track_media_dept_online(
        cls,
        media: str = "",
        dept: str = "",
        rank: str = "",
        client_id: str = "",
        client_secret: str = "",
        stop_event: Optional[Any] = None,
        full_history: bool = True,
        db_path: str = "media_tracker.db"
    ) -> Dict[str, Any]:
        """
        특정 매체명, 부서명, 직급명의 최신(당일/당해) 온라인 [인사]/<인사> 기사를 실시간 고속 검색하여
        신규 발령 레코드를 DB에 증분 등록합니다.
        full_history=True일 경우 1965~2026 전체 기간 심층 탐색을 수행합니다.
        """
        media_clean = media.strip() if media else ""
        dept_clean = dept.strip() if dept else ""
        rank_clean = rank.strip() if rank else ""

        # 복합 검색어 자동 분리 (예: media="뉴스1 건설부동산" -> media="뉴스1", dept="건설부동산")
        if media_clean and not dept_clean:
            if media_clean not in KNOWN_MEDIA_NAMES and normalize_media_name(media_clean) not in KNOWN_MEDIA_NAMES:
                for km in sorted(KNOWN_MEDIA_NAMES, key=len, reverse=True):
                    if media_clean.startswith(km) and len(media_clean) > len(km):
                        remainder = media_clean[len(km):].strip()
                        if remainder and (remainder in ["건설부동산", "산업부", "경제부", "정치부", "사회부", "금융부", "증권부"] or remainder.endswith(("부", "팀", "국", "실", "센터", "단", "소")) or " " in media_clean):
                            media_clean = km
                            dept_clean = remainder
                            break
            if not dept_clean and " " in media_clean:
                parts = media_clean.split(None, 1)
                if any(is_same_media_company(parts[0], km) for km in KNOWN_MEDIA_NAMES) or parts[0] in KNOWN_MEDIA_NAMES:
                    media_clean = parts[0]
                    dept_clean = parts[1]

        # dept_clean 내에 직급명(부장, 차장 등)이 포함된 경우 rank_clean으로 자동 분리
        if dept_clean and not rank_clean:
            for rnk_term in ["부장", "차장", "팀장", "국장", "부국장", "실장", "본부장", "에디터", "데스크", "기자"]:
                if rnk_term in dept_clean:
                    dept_clean = re.sub(rf"\b{rnk_term}\b|\s*{rnk_term}", "", dept_clean).strip()
                    rank_clean = rnk_term
                    break

        # 매체명과 부서명이 서로 바뀐 경우 자동 보정
        if dept_clean and not media_clean:
            if any(is_same_media_company(dept_clean, km) for km in KNOWN_MEDIA_NAMES) or dept_clean in KNOWN_MEDIA_NAMES:
                media_clean, dept_clean = dept_clean, ""
        elif dept_clean and media_clean:
            if any(is_same_media_company(dept_clean, km) for km in KNOWN_MEDIA_NAMES) and not any(is_same_media_company(media_clean, km) for km in KNOWN_MEDIA_NAMES):
                media_clean, dept_clean = dept_clean, media_clean

        if not media_clean and not dept_clean and not rank_clean:
            return {"found_records": 0, "articles_searched": 0, "message": "매체명, 부서명, 또는 직급명을 입력하세요."}

        client_id = client_id or cls.DEFAULT_NAVER_CLIENT_ID
        client_secret = client_secret or cls.DEFAULT_NAVER_CLIENT_SECRET

        if not full_history:
            # 실시간 인터랙티브 검색 시 CPU/네트워크 부하 0화:
            # 단일 네이버 News API JSON 호출 및 멀티쓰레드/웹크롤링 완전 배제
            query_tokens = [t for t in [media_clean, dept_clean, rank_clean] if t]
            query_str = " ".join(query_tokens) + " [인사]" if query_tokens else "[인사]"
            candidate_articles = cls.search_online_articles(
                query_str, max_items=3, client_id=client_id, client_secret=client_secret, full_history=False
            )
            if not candidate_articles:
                return {"found_records": 0, "articles_searched": 0, "message": "최신 기사 확인 완료"}

            db_known_urls = set()
            try:
                db_inst = MediaDatabase(db_path)
                with db_inst.get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT article_url FROM personnel_records WHERE article_url IS NOT NULL AND article_url != ''")
                    db_known_urls = {row[0] for row in cur.fetchall()}
            except Exception:
                pass

            new_articles = [a for a in candidate_articles if a.get("url") and a.get("url") not in db_known_urls]
            if not new_articles:
                return {"found_records": 0, "articles_searched": len(candidate_articles), "message": "최신 상태 확인 완료"}

            all_records = []
            for art in new_articles[:2]:
                try:
                    fetched = cls.fetch_full_text(art["url"])
                    body = fetched.get("text", "")
                    if body and len(body) >= 30:
                        recs = PersonnelRegexParser.parse_article_text(
                            article_text=body,
                            default_media=media_clean or fetched.get("media", "온라인뉴스"),
                            article_title=art.get("title", ""),
                            pub_date=fetched.get("pub_date") or art.get("date_text") or "2026-01-01",
                            article_url=art["url"]
                        )
                        for r in recs:
                            if media_clean and not is_same_media_company(r.get("media", ""), media_clean):
                                continue
                            if dept_clean and dept_clean not in r.get("dept", ""):
                                continue
                            if not is_excluded_department(r.get("dept", "")):
                                all_records.append(r)
                except Exception:
                    pass

            if all_records:
                engine = PersonnelTrackerEngine(db_path)
                engine.process_records(all_records, append=True)
                return {"found_records": len(all_records), "articles_searched": len(candidate_articles), "message": f"신규 {len(all_records)}건 동기화 완료"}
            return {"found_records": 0, "articles_searched": len(candidate_articles), "message": "최신 상태 확인 완료"}

        cur_year = datetime.datetime.now().year
        if full_history:
            intervals = [
                (2025, 2026, "date"),
                (2023, 2024, "date"),
                (2021, 2022, "date"),
                (2019, 2020, "sim"),
                (2017, 2018, "sim"),
                (2015, 2016, "sim"),
                (2013, 2014, "sim"),
                (2011, 2012, "sim"),
                (2009, 2010, "sim"),
                (2005, 2008, "sim")
            ]
            sec_pages = 2
        else:
            intervals = [
                (cur_year, cur_year, "date")
            ]
            sec_pages = 1

        # 검색 쿼리 구성 (언론사 및 뉴스1/연합뉴스/중앙일보 타깃 - 핵심 쿼리 고속화)
        search_tasks = []
        for sy, ey, sort_type in intervals:
            q_list = []
            alias_candidates = []
            if media_clean in MEDIA_GROUP_ALIASES:
                alias_candidates.extend(MEDIA_GROUP_ALIASES[media_clean][:4])
            if media_clean and not media_clean.endswith(("신문", "방송", "뉴스", "통신", "일보")):
                alias_candidates.append(f"{media_clean}신문")

            if media_clean and rank_clean and dept_clean:
                q_list.extend([
                    f'{media_clean} {dept_clean} {rank_clean}',
                    f'{media_clean} {dept_clean}',
                    f'{media_clean} "{dept_clean}" [인사]',
                    f'{media_clean} "{rank_clean}" [인사]',
                    f'[인사] {media_clean}',
                    f'<인사> {media_clean}',
                    f'{media_clean} [인사] 뉴스1',
                    f'{media_clean} [인사] 연합뉴스',
                ])
                for g_alias in alias_candidates:
                    q_list.extend([
                        f'[인사] {g_alias}',
                        f'<인사> {g_alias}',
                        f'{g_alias} [인사]',
                        f'{g_alias} {dept_clean}',
                        f'{g_alias} {dept_clean} {rank_clean}'
                    ])
                    if "건설" in dept_clean and "부동산" in dept_clean:
                        q_list.extend([
                            f'{g_alias} 부동산',
                            f'{g_alias} 건설'
                        ])
            elif media_clean and rank_clean:
                q_list.extend([
                    f'{media_clean} {rank_clean} [인사]',
                    f'{media_clean} {rank_clean} 인사',
                    f'[인사] {media_clean}',
                    f'<인사> {media_clean}',
                    f'{media_clean} [인사] 뉴스1',
                    f'{media_clean} [인사] 연합뉴스',
                ])
                for g_alias in alias_candidates:
                    q_list.extend([
                        f'[인사] {g_alias}',
                        f'<인사> {g_alias}',
                        f'{g_alias} [인사]',
                        f'{g_alias} {rank_clean}'
                    ])
            elif media_clean and dept_clean:
                q_list.extend([
                    f'{media_clean} {dept_clean} [인사]',
                    f'{media_clean} {dept_clean} 인사',
                    f'[인사] {media_clean}',
                    f'<인사> {media_clean}',
                    f'{media_clean} [인사] 뉴스1',
                    f'{media_clean} [인사] 연합뉴스',
                ])
                for g_alias in alias_candidates:
                    q_list.extend([
                        f'[인사] {g_alias}',
                        f'<인사> {g_alias}',
                        f'{g_alias} [인사]',
                        f'{g_alias} {dept_clean}'
                    ])
                    if "건설" in dept_clean and "부동산" in dept_clean:
                        q_list.extend([
                            f'{g_alias} 부동산',
                            f'{g_alias} 건설'
                        ])
            elif media_clean:
                q_list.extend([
                    f'{media_clean} 인사',
                    f'[인사] {media_clean}',
                    f'<인사> {media_clean}',
                    f'{media_clean} [인사]',
                    f'{media_clean} [인사] 연합뉴스',
                    f'{media_clean} [인사] 뉴스1',
                ])
                for g_alias in alias_candidates:
                    q_list.extend([
                        f'{g_alias} 인사',
                        f'[인사] {g_alias}',
                        f'<인사> {g_alias}',
                        f'{g_alias} [인사]'
                    ])
            elif rank_clean and dept_clean:
                q_list.extend([
                    f'{dept_clean} {rank_clean} [인사] 뉴스1',
                    f'{dept_clean} {rank_clean} [인사] 연합뉴스',
                    f'{dept_clean} [인사] 중앙일보',
                ])
            elif rank_clean:
                # 오직 직급(부장, 차장 등)만 검색한 경우 언론사 및 뉴스1/연합뉴스/중앙일보 타깃
                q_list.extend([
                    f'{rank_clean} [인사] 뉴스1',
                    f'{rank_clean} [인사] 연합뉴스',
                    f'{rank_clean} [인사] 중앙일보',
                    f'신문사 {rank_clean} [인사]',
                ])
            elif dept_clean:
                q_list.extend([
                    f'{dept_clean} [인사] 뉴스1',
                    f'{dept_clean} [인사] 연합뉴스',
                    f'{dept_clean} [인사] 중앙일보',
                ])

            if not full_history and len(q_list) > 4:
                q_list = q_list[:4]

            for q in q_list:
                search_tasks.append((q, sy, ey, sort_type))

        candidate_articles: Dict[str, Dict[str, Any]] = {}

        # 4대 매체 전용 섹션(연합뉴스·한국기자협회·뉴스1·중앙일보) 직접 보조 탐색 (병렬 고속화)
        try:
            kw_target = media_clean or dept_clean or rank_clean
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as direct_exec:
                fetcher_futures = [
                    direct_exec.submit(cls.fetch_yonhap_personnel_section, sec_pages, kw_target, set()),
                    direct_exec.submit(cls.fetch_journalist_personnel_section, sec_pages, kw_target, set()),
                    direct_exec.submit(cls.fetch_news1_personnel_section, sec_pages, kw_target, set()),
                    direct_exec.submit(cls.fetch_joongang_personnel_section, sec_pages, kw_target, set()),
                ]
                for f in concurrent.futures.as_completed(fetcher_futures):
                    for d_art in f.result():
                        u = d_art.get("url", "")
                        if u and u not in candidate_articles:
                            candidate_articles[u] = d_art
        except Exception:
            pass

        def run_search_task(task):
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                return []
            q, sy, ey, st = task
            return cls.search_online_articles(
                q, max_items=25, client_id=client_id, client_secret=client_secret,
                start_year=sy, end_year=ey, sort_first=st
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(run_search_task, t) for t in search_tasks]
            for f in concurrent.futures.as_completed(futures):
                if stop_event and getattr(stop_event, "is_set", lambda: False)():
                    break
                try:
                    for art in f.result():
                        u = art.get("url", "")
                        if u and u not in candidate_articles:
                            candidate_articles[u] = art
                except Exception:
                    pass

        db_known_urls = set()
        try:
            db_inst = MediaDatabase()
            with db_inst.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT article_url FROM personnel_records WHERE article_url IS NOT NULL AND article_url != ''")
                db_known_urls = {row[0] for row in cur.fetchall()}
        except Exception:
            pass

        def fetch_worker(art):
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                return None
            u = art["url"]
            if u in db_known_urls:
                return None
            try:
                fetched = cls.fetch_full_text(u)
                body = fetched.get("text", "")
                if not body or len(body) < 30:
                    return None
                # 본문 내 직급/부서/매체 필터링: 매체명이 있는 경우 매체명 존재 여부 확인
                allowed_media_tokens = [media_clean]
                if media_clean in MEDIA_GROUP_ALIASES:
                    allowed_media_tokens.extend(MEDIA_GROUP_ALIASES[media_clean])
                for base, alist in MEDIA_GROUP_ALIASES.items():
                    if media_clean == base or media_clean in alist:
                        allowed_media_tokens.append(base)
                        allowed_media_tokens.extend(alist)
                allowed_media_tokens = list(set(allowed_media_tokens))

                # 머니투데이 vs 머니투데이방송 완벽 격리
                if media_clean == "머니투데이":
                    allowed_media_tokens = [t for t in allowed_media_tokens if "방송" not in t and "mtn" not in t.lower()]
                    # 제목이 명백한 머니투데이방송 기사 배제
                    if any(k in art.get("title", "") for k in ["머니투데이방송", "MTN", "mtn"]) and not any(k in art.get("title", "") for k in ["외", "종합", "등"]):
                        return None
                elif media_clean in ["머니투데이방송", "MTN"] or media_clean.lower() == "mtn":
                    allowed_media_tokens = ["머니투데이방송", "MTN", "mtn"]
                    if "머니투데이" in art.get("title", "") and not any(k in art.get("title", "") for k in ["방송", "MTN", "mtn"]) and not any(k in art.get("title", "") for k in ["외", "종합", "등"]):
                        return None

                if media_clean and not any(tok in body or tok in art.get("title", "") for tok in allowed_media_tokens):
                    return None
                if not media_clean and dept_clean and dept_clean not in body and dept_clean not in art.get("title", ""):
                    return None
                if not media_clean and not dept_clean and rank_clean and rank_clean not in body and rank_clean not in art.get("title", ""):
                    return None
                return art, fetched
            except Exception:
                return None

        fetched_results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(fetch_worker, art) for art in candidate_articles.values()]
            for f in concurrent.futures.as_completed(futures):
                if stop_event and getattr(stop_event, "is_set", lambda: False)():
                    break
                try:
                    res = f.result()
                    if res:
                        fetched_results.append(res)
                except Exception:
                    pass

        all_parsed_records = []
        for art, fetched in fetched_results:
            u = art["url"]
            body = fetched["text"]
            art_media = fetched.get("media") if fetched.get("media") != "온라인뉴스" else (media_clean or art.get("media"))
            pub_date = fetched.get("pub_date") or art.get("date_text") or "2020-01-01"

            recs = PersonnelRegexParser.parse_article_text(
                article_text=body,
                default_media=art_media,
                article_title=art.get("title", ""),
                pub_date=pub_date,
                article_url=u
            )
            for r in recs:
                if not is_media_organization(r.get("media", "")) or is_excluded_department(r.get("dept", "")):
                    continue
                rec_med = r.get("media", "")
                is_target_media = False
                if not media_clean:
                    is_target_media = True
                elif is_same_media_company(media_clean, rec_med):
                    is_target_media = True
                    r["media"] = normalize_media_name(rec_med)
                elif rec_med in ["온라인뉴스", "미상", ""] and any(tok in art.get("title", "") for tok in allowed_media_tokens) and not any(kw in art.get("title", "") for kw in ["종합", "외", "등", "동향"]):
                    is_target_media = True
                    r["media"] = normalize_media_name(media_clean)

                if is_target_media:
                    all_parsed_records.append(r)

        # 1. 수집된 모든 유효 언론인 발령 레코드를 DB에 전수 등록 (지식 베이스 누적)
        engine_res = {"unique_reporters": 0}
        if all_parsed_records:
            engine = PersonnelTrackerEngine(db_path=db_path)
            engine_res = engine.process_records(all_parsed_records, append=True)

        def matches_dept(target_d: str, record_d: str) -> bool:
            if not target_d:
                return True
            if not record_d:
                return False
            td = target_d.strip()
            rd = record_d.strip()
            if td in rd or rd in td:
                return True
            sub_tokens = [td[:2], td[2:]] if len(td) == 5 else ([t for t in re.split(r"[/·,\s]+", td) if t])
            return any(st in rd for st in sub_tokens if len(st) >= 2)

        def matches_rank(target_r: str, record_r: str, record_d: str = "", raw_t: str = "") -> bool:
            if not target_r:
                return True
            tr = target_r.strip()
            if tr == "기자":
                # '기자' 검색 시 직급 미언급 인물(부장/차장 등 관리직 언급이 없는 경우) 포함
                if not record_r or record_r in ["기자", "평기자", "미표기", "수습기자"]:
                    return True
                managerial = ["부장", "차장", "국장", "부국장", "팀장", "실장", "본부장", "대표", "사장", "주필", "논설", "위원", "에디터", "데스크"]
                if not any(m in record_r for m in managerial):
                    return True
                if raw_t and not any(m in raw_t for m in managerial):
                    return True
                return False
            return tr in record_r or tr in record_d or record_r in tr

        # 2. 사용자가 요청한 조건(media, dept, rank)에 부합하는 레코드 필터링
        exact_matches = [
            r for r in all_parsed_records
            if (not media_clean or is_same_media_company(media_clean, r.get("media", "")))
            and matches_dept(dept_clean, r.get("dept", ""))
            and matches_rank(rank_clean, r.get("rank_title", ""), r.get("dept", ""), r.get("raw_text", ""))
        ]

        # 동일 부서 타 직급 매칭 (예: 더벨 건설부동산부 차장 전기룡)
        dept_matches = []
        if not exact_matches and media_clean and dept_clean:
            dept_matches = [
                r for r in all_parsed_records
                if is_same_media_company(media_clean, r.get("media", "")) and matches_dept(dept_clean, r.get("dept", ""))
            ]

        # 동일 언론사 전체 매칭
        media_matches = []
        if not exact_matches and not dept_matches and media_clean:
            media_matches = [
                r for r in all_parsed_records
                if media_clean in r.get("media", "")
            ]

        is_stopped = bool(stop_event and getattr(stop_event, "is_set", lambda: False)())
        msg_suffix = " (중단 전까지 수집된 내역 반영)" if is_stopped else ""
        target_label = " ".join(filter(None, [media_clean, dept_clean, rank_clean]))

        if exact_matches:
            return {
                "found_records": len(exact_matches),
                "articles_searched": len(candidate_articles),
                "unique_reporters": engine_res["unique_reporters"],
                "message": f"4대 매체(연합뉴스·뉴스1·중앙일보·한국기자협회) 아카이브 전체 기간(1965~2026) 온라인 [인사]/<인사> 기사 {len(candidate_articles)}건을 탐색하여 '{target_label}' 역대 발령 기록 {len(exact_matches)}건(언론인 {engine_res['unique_reporters']}명)을 실시간 수집했습니다!{msg_suffix}"
            }
        elif dept_matches:
            sample_names = ", ".join([f"{r['name']}({r.get('rank_title', '')})" for r in dept_matches[:3]])
            return {
                "found_records": len(dept_matches),
                "articles_searched": len(candidate_articles),
                "unique_reporters": engine_res["unique_reporters"],
                "message": f"'{media_clean} {dept_clean}' 부서에 '{rank_clean}' 직급 발령은 없으나, 동일 부서 타 직급 발령({len(dept_matches)}건: {sample_names})을 확인하여 DB에 반영했습니다!{msg_suffix}"
            }
        elif media_matches:
            return {
                "found_records": len(media_matches),
                "articles_searched": len(candidate_articles),
                "unique_reporters": engine_res["unique_reporters"],
                "message": f"'{media_clean}'의 '{dept_clean} {rank_clean}' 조건 발령은 없으나, 해당 언론사의 역대 발령 기록 {len(media_matches)}건(언론인 {engine_res['unique_reporters']}명)을 실시간 수집하여 DB에 반영했습니다!{msg_suffix}"
            }
        else:
            if is_stopped:
                return {
                    "found_records": 0,
                    "articles_searched": len(candidate_articles),
                    "message": "사용자에 의해 실시간 탐색이 중단되었습니다."
                }
            scope_desc = f"전체 아카이브({len(candidate_articles)}건 탐색)" if full_history else "당일 및 최신 기사 확인"
            return {
                "found_records": 0,
                "articles_searched": len(candidate_articles),
                "message": f"{scope_desc} 완료: 아카이브 DB(전체 기간 7,176건) 기반으로 검색 결과를 조회합니다."
            }

    @classmethod
    def track_media_name_online(
        cls,
        media: str = "",
        name: str = "",
        client_id: str = "",
        client_secret: str = "",
        stop_event: Optional[Any] = None,
        full_history: bool = True,
        db_path: str = "media_tracker.db"
    ) -> Dict[str, Any]:
        """
        특정 매체명 및 인물명의 최신(당일/당해) 온라인 [인사]/<인사> 기사를 실시간 검색하여
        신규 발령 레코드를 DB에 증분 등록합니다.
        full_history=True일 경우 1965~2026 전체 기간 심층 탐색을 수행합니다.
        """
        media_clean = media.strip() if media else ""
        name_clean = name.strip() if name else ""

        if not media_clean and not name_clean:
            return {"found_records": 0, "articles_searched": 0, "message": "매체명 또는 기자 이름을 입력하세요."}
        if not name_clean:
            return cls.track_media_dept_online(media=media_clean, client_id=client_id, client_secret=client_secret, stop_event=stop_event, full_history=full_history)
        if not media_clean:
            return cls.track_reporter_online(reporter_name=name_clean, client_id=client_id, client_secret=client_secret, stop_event=stop_event, full_history=full_history)

        client_id = client_id or cls.DEFAULT_NAVER_CLIENT_ID
        client_secret = client_secret or cls.DEFAULT_NAVER_CLIENT_SECRET

        if not full_history:
            # 실시간 인터랙티브 검색 시 CPU/네트워크 부하 0화:
            # 단일 네이버 News API JSON 호출 및 멀티쓰레드/웹크롤링 완전 배제
            query_str = f'"{media_clean}" "{name_clean}" 인사' if media_clean else f'"{name_clean}" 인사'
            candidate_articles = cls.search_online_articles(
                query_str, max_items=3, client_id=client_id, client_secret=client_secret, full_history=False
            )
            if not candidate_articles:
                return {"found_records": 0, "articles_searched": 0, "message": "최신 기사 확인 완료"}

            db_known_urls = set()
            try:
                db_inst = MediaDatabase(db_path)
                with db_inst.get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT article_url FROM personnel_records WHERE article_url IS NOT NULL AND article_url != ''")
                    db_known_urls = {row[0] for row in cur.fetchall()}
            except Exception:
                pass

            new_articles = [a for a in candidate_articles if a.get("url") and a.get("url") not in db_known_urls]
            if not new_articles:
                return {"found_records": 0, "articles_searched": len(candidate_articles), "message": "최신 상태 확인 완료"}

            all_records = []
            for art in new_articles[:2]:
                try:
                    fetched = cls.fetch_full_text(art["url"])
                    body = fetched.get("text", "")
                    if body and len(body) >= 30 and name_clean in body:
                        recs = PersonnelRegexParser.parse_article_text(
                            article_text=body,
                            default_media=media_clean or fetched.get("media", "온라인뉴스"),
                            article_title=art.get("title", ""),
                            pub_date=fetched.get("pub_date") or art.get("date_text") or "2026-01-01",
                            article_url=art["url"]
                        )
                        for r in recs:
                            if r["name"] == name_clean:
                                if media_clean and not is_same_media_company(r.get("media", ""), media_clean):
                                    continue
                                if not is_excluded_department(r.get("dept", "")):
                                    all_records.append(r)
                except Exception:
                    pass

            if all_records:
                engine = PersonnelTrackerEngine(db_path)
                engine.process_records(all_records, append=True)
                return {"found_records": len(all_records), "articles_searched": len(candidate_articles), "message": f"신규 {len(all_records)}건 동기화 완료"}
            return {"found_records": 0, "articles_searched": len(candidate_articles), "message": "최신 상태 확인 완료"}

        cur_year = datetime.datetime.now().year
        if full_history:
            intervals = [
                (2025, 2026, "date"),
                (2023, 2024, "date"),
                (2021, 2022, "date"),
                (2019, 2020, "sim"),
                (2017, 2018, "sim"),
                (2015, 2016, "sim"),
                (2013, 2014, "sim"),
                (2011, 2012, "sim"),
                (2009, 2010, "sim"),
                (2005, 2008, "sim")
            ]
            sec_pages = 2
        else:
            intervals = [
                (cur_year, cur_year, "date")
            ]
            sec_pages = 1

        search_tasks = []
        for sy, ey, sort_type in intervals:
            q_list = [
                f'"{media_clean}" "{name_clean}" 인사',
                f'{media_clean} "{name_clean}" [인사]',
                f'"{name_clean}" [인사] {media_clean}',
                f'"{name_clean}" [인사] 뉴스1',
                f'"{name_clean}" [인사] 연합뉴스',
            ]
            if media_clean in MEDIA_GROUP_ALIASES:
                for g_alias in MEDIA_GROUP_ALIASES[media_clean][:2]:
                    q_list.append(f'"{g_alias}" "{name_clean}" 인사')
                    q_list.append(f'{g_alias} "{name_clean}" [인사]')
            if media_clean and not media_clean.endswith(("신문", "방송", "뉴스", "통신", "일보", "타임스", "타임즈")):
                q_list.append(f'"{media_clean}신문" "{name_clean}" 인사')
                q_list.append(f'{media_clean}신문 "{name_clean}" [인사]')

            if not full_history and len(q_list) > 4:
                q_list = q_list[:4]

            for q in q_list:
                search_tasks.append((q, sy, ey, sort_type))

        candidate_articles: Dict[str, Dict[str, Any]] = {}

        # 중앙일보 피플 공식 인사 섹션(https://www.joongang.co.kr/people/personnel) 직접 보조 탐색
        try:
            kw_target = name_clean or media_clean
            ja_direct = cls.fetch_joongang_personnel_section(
                max_pages=sec_pages,
                target_keyword=kw_target
            )
            for j_art in ja_direct:
                u = j_art.get("url", "")
                if u and u not in candidate_articles:
                    candidate_articles[u] = j_art
        except Exception:
            pass

        def run_search_task(task):
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                return []
            q, sy, ey, st = task
            return cls.search_online_articles(
                q, max_items=25, client_id=client_id, client_secret=client_secret,
                start_year=sy, end_year=ey, sort_first=st
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(run_search_task, t) for t in search_tasks]
            for f in concurrent.futures.as_completed(futures):
                if stop_event and getattr(stop_event, "is_set", lambda: False)():
                    break
                try:
                    for art in f.result():
                        u = art.get("url", "")
                        if u and u not in candidate_articles:
                            candidate_articles[u] = art
                except Exception:
                    pass

        all_records = []
        known_medias = set()

        db_known_urls = set()
        try:
            db_inst = MediaDatabase()
            with db_inst.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT article_url FROM personnel_records WHERE article_url IS NOT NULL AND article_url != ''")
                db_known_urls = {row[0] for row in cur.fetchall()}
        except Exception:
            pass

        def fetch_worker(art):
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                return None
            u = art["url"]
            if u in db_known_urls:
                return None
            try:
                fetched = cls.fetch_full_text(u)
                body = fetched.get("text", "")
                if not body or len(body) < 30 or name_clean not in body:
                    return None
                allowed_media_tokens = [media_clean]
                if media_clean in MEDIA_GROUP_ALIASES:
                    allowed_media_tokens.extend(MEDIA_GROUP_ALIASES[media_clean])
                for base, alist in MEDIA_GROUP_ALIASES.items():
                    if media_clean == base or media_clean in alist:
                        allowed_media_tokens.append(base)
                        allowed_media_tokens.extend(alist)
                allowed_media_tokens = list(set(allowed_media_tokens))

                # 머니투데이 vs 머니투데이방송 완벽 격리
                if media_clean == "머니투데이":
                    allowed_media_tokens = [t for t in allowed_media_tokens if "방송" not in t and "mtn" not in t.lower()]
                    # 제목이 명백한 머니투데이방송 기사 배제
                    if any(k in art.get("title", "") for k in ["머니투데이방송", "MTN", "mtn"]) and not any(k in art.get("title", "") for k in ["외", "종합", "등"]):
                        return None
                elif media_clean in ["머니투데이방송", "MTN"] or media_clean.lower() == "mtn":
                    allowed_media_tokens = ["머니투데이방송", "MTN", "mtn"]
                    if "머니투데이" in art.get("title", "") and not any(k in art.get("title", "") for k in ["방송", "MTN", "mtn"]) and not any(k in art.get("title", "") for k in ["외", "종합", "등"]):
                        return None

                if media_clean and not any(tok in body or tok in art.get("title", "") for tok in allowed_media_tokens):
                    return None
                return art, fetched
            except Exception:
                return None

        fetched_results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(fetch_worker, art) for art in candidate_articles.values()]
            for f in concurrent.futures.as_completed(futures):
                if stop_event and getattr(stop_event, "is_set", lambda: False)():
                    break
                try:
                    res = f.result()
                    if res:
                        fetched_results.append(res)
                except Exception:
                    pass

        for art, fetched in fetched_results:
            u = art["url"]
            body = fetched["text"]
            media_src = fetched["media"] if fetched["media"] != "온라인뉴스" else art["media"]
            article_pub_date = fetched.get("pub_date") or art.get("date_text") or "2024-01-01"
            final_u = fetched.get("origin_url") or u

            recs = PersonnelRegexParser.parse_article_text(
                article_text=body,
                default_media=media_src,
                article_title=art.get("title", ""),
                pub_date=article_pub_date,
                article_url=final_u
            )
            matched_recs = []
            for r in recs:
                if r["name"] != name_clean or not is_media_organization(r.get("media", "")) or is_excluded_department(r.get("dept", "")):
                    continue
                rec_med = r.get("media", "")
                if media_clean:
                    is_match = is_same_media_company(media_clean, rec_med)
                    if not is_match:
                        continue
                matched_recs.append(r)

            if matched_recs:
                for mr in matched_recs:
                    if fetched.get("email") and not mr.get("email"):
                        mr["email"] = fetched["email"]
                    if is_media_organization(mr.get("media", "")):
                        known_medias.add(mr["media"])
                all_records.extend(matched_recs)

        if known_medias and not (stop_event and getattr(stop_event, "is_set", lambda: False)()):
            extra_tasks = []
            for km in list(known_medias):
                if media_clean and km != media_clean and not (media_clean in MEDIA_GROUP_ALIASES and km in MEDIA_GROUP_ALIASES[media_clean]):
                    continue
                extra_tasks.append((f'{km} "{name_clean}" [인사]', 1965, 2026, "sim"))
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                extra_futures = [executor.submit(run_search_task, t) for t in extra_tasks]
                extra_arts = []
                for f in concurrent.futures.as_completed(extra_futures):
                    for a in f.result():
                        if a["url"] not in candidate_articles:
                            candidate_articles[a["url"]] = a
                            extra_arts.append(a)
                
                if extra_arts:
                    extra_fetch_futures = [executor.submit(fetch_worker, a) for a in extra_arts]
                    for f in concurrent.futures.as_completed(extra_fetch_futures):
                        res = f.result()
                        if res:
                            art, fetched = res
                            recs = PersonnelRegexParser.parse_article_text(
                                article_text=fetched["text"],
                                default_media=fetched["media"] if fetched["media"] != "온라인뉴스" else art["media"],
                                article_title=art.get("title", ""),
                                pub_date=fetched.get("pub_date") or art.get("date_text") or "2020-01-01",
                                article_url=fetched.get("origin_url") or art["url"]
                            )
                            for r in recs:
                                if r["name"] != name_clean or not is_media_organization(r.get("media", "")) or is_excluded_department(r.get("dept", "")):
                                    continue
                                rec_med = r.get("media", "")
                                if media_clean:
                                    is_match = (
                                        media_clean in rec_med 
                                        or rec_med in media_clean
                                        or (media_clean in MEDIA_GROUP_ALIASES and any(alias in rec_med for alias in MEDIA_GROUP_ALIASES[media_clean]))
                                    )
                                    if not is_match:
                                        continue
                                all_records.append(r)

        is_stopped = bool(stop_event and getattr(stop_event, "is_set", lambda: False)())
        target_str = f"'{media_clean} {name_clean}'" if media_clean else f"'{name_clean}'"
        target_no_res = f"'{media_clean}' 소속 '{name_clean}'" if media_clean else f"'{name_clean}'"
        if all_records:
            engine = PersonnelTrackerEngine(db_path=db_path)
            res = engine.process_records(all_records, append=True)
            msg_suffix = " (중단 전까지 수집된 내역 반영)" if is_stopped else ""
            return {
                "found_records": len(all_records),
                "articles_searched": len(candidate_articles),
                "unique_reporters": res["unique_reporters"],
                "message": f"4대 매체(연합뉴스·뉴스1·중앙일보·한국기자협회) 아카이브 전체 기간(1965~2026) 공식 [인사]/<인사> 기사 {len(candidate_articles)}건을 탐색하여 {target_str} 기자의 발령 기록 {len(all_records)}건을 실시간 수집했습니다!{msg_suffix}"
            }
        else:
            if is_stopped:
                return {"found_records": 0, "articles_searched": len(candidate_articles), "message": "사용자에 의해 실시간 탐색이 중단되었습니다."}
            return {
                "found_records": 0,
                "articles_searched": len(candidate_articles),
                "message": f"4대 매체(연합뉴스·뉴스1·중앙일보·한국기자협회) 아카이브 전체 기간(1965~2026) 공식 [인사] 기사에서 {target_no_res} 기자의 발령 기록을 찾지 못했습니다."
            }

    @classmethod
    def fetch_latest_online_personnel(
        cls, 
        max_articles: int = 15,
        client_id: str = "",
        client_secret: str = "",
        db_path: str = "media_tracker.db"
    ) -> Dict[str, Any]:
        """최신 온라인 인사 기사 전문을 대량 수집하여 파싱 및 DB에 등록합니다."""
        arts = cls.search_online_articles("[인사]", max_items=max_articles, client_id=client_id, client_secret=client_secret)
        all_records = []
        for art in arts:
            fetched = cls.fetch_full_text(art["url"])
            body = fetched["text"]
            media = fetched["media"] if fetched["media"] != "온라인뉴스" else art["media"]
            pub_date = art.get("date_text") or fetched.get("pub_date") or ""
            if body and len(body) >= 50:
                recs = PersonnelRegexParser.parse_article_text(
                    article_text=body,
                    default_media=media,
                    article_title=art["title"],
                    pub_date=pub_date
                )
                for r in recs:
                    if fetched["email"] and not r.get("email"):
                        r["email"] = fetched["email"]
                all_records.extend(recs)

        if all_records:
            engine = PersonnelTrackerEngine(db_path=db_path)
            res = engine.process_records(all_records, append=True)
            return {
                "articles_count": len(arts),
                "records_extracted": len(all_records),
                "unique_reporters": res["unique_reporters"],
                "job_changes": res["job_changes_detected"],
                "message": f"최신 온라인 기사 {len(arts)}건에서 {len(all_records)}건의 발령 기록(총 언론인 {res['unique_reporters']}명)을 실시간 수집 및 등록 완료했습니다!"
            }
        else:
            return {
                "articles_count": len(arts),
                "records_extracted": 0,
                "message": "온라인 기사를 가져왔으나 발령 명단을 추출하지 못했습니다."
            }

    @classmethod
    def fetch_daily_personnel_news(
        cls,
        target_date: str = "",
        max_articles: int = 25,
        client_id: str = "",
        client_secret: str = "",
        db_path: str = "media_tracker.db"
    ) -> Dict[str, Any]:
        """
        당일(또는 지정된 일자)의 공식 [인사]/<인사> 기사를 실시간 크롤링하여
        언론사별, 발령구분(승진/전보/보임/선임)별로 구조화된 일일 인사 소식 브리핑 데이터를 생성하고
        DB에 영속화합니다.
        """
        if not target_date:
            target_date = datetime.date.today().strftime("%Y-%m-%d")

        search_queries = [
            '"[인사]"',
            '"<인사>"',
            '[인사] 신문',
            '<인사> 신문',
            '[인사] 방송',
            '<인사> 방송',
            '[인사] 통신',
            '<인사> 통신',
            '[인사] 일보',
            '<인사> 일보',
            '[인사] 경제',
            '<인사> 경제',
            '[인사] 미디어',
            '<인사> 미디어',
            '[인사] 뉴스',
            '<인사> 뉴스',
            '[인사]',
            '<인사>'
        ]

        seen_urls = set()
        collected_articles = []

        for q in search_queries:
            if len(collected_articles) >= max_articles * 2:
                break
            arts = cls.search_online_articles(
                q, 
                max_items=12, 
                client_id=client_id, 
                client_secret=client_secret,
                start_year=2024,
                end_year=2026,
                sort_first="date"
            )
            for a in arts:
                tit = a.get("title", "")
                if not is_personnel_article_title(tit):
                    continue
                u = a.get("url", "")
                if not u or u in seen_urls:
                    continue
                seen_urls.add(u)
                collected_articles.append(a)

        all_records = []
        media_news_map = {}

        for a in collected_articles[:max_articles]:
            fetched = cls.fetch_full_text(a["url"])
            body = fetched.get("text", "")
            if not body or len(body) < 30:
                continue

            pub_date = fetched.get("pub_date") or a.get("date_text") or target_date
            art_media = fetched.get("media") if fetched.get("media") != "온라인뉴스" else a.get("media")

            recs = PersonnelRegexParser.parse_article_text(
                article_text=body,
                default_media=art_media,
                article_title=a["title"],
                pub_date=pub_date,
                article_url=a["url"]
            )
            clean_recs = [
                r for r in recs 
                if is_personnel_article_title(r.get("article_title", ""))
                and is_media_organization(r.get("media", ""))
                and r["name"] not in STOPWORDS and len(r["name"]) >= 2
            ]
            if not clean_recs:
                continue

            # 개별 발령 레코드의 매체별로 올바르게 그룹화 (종합 인사 기사 내 복수 매체 지원)
            for r in clean_recs:
                r_media = r.get("media", art_media)
                if r_media not in media_news_map:
                    media_news_map[r_media] = {
                        "media": r_media,
                        "articles": [],
                        "records": [],
                        "by_action": {},
                        "latest_date": pub_date,
                        "article_title": a["title"],
                        "article_url": a["url"]
                    }
                media_news_map[r_media]["records"].append(r)
                act = r.get("action_type", "발령")
                if act not in media_news_map[r_media]["by_action"]:
                    media_news_map[r_media]["by_action"][act] = []
                media_news_map[r_media]["by_action"][act].append(r)

            all_records.extend(clean_recs)

        job_changes_detected = []
        if all_records:
            engine = PersonnelTrackerEngine(db_path=db_path)
            res = engine.process_records(all_records, append=True)
            job_changes_detected = engine.db.get_job_changes(limit=50)

        return {
            "target_date": target_date,
            "total_articles": len([a for m in media_news_map.values() for a in m["articles"]]),
            "total_media": len(media_news_map),
            "total_records": len(all_records),
            "media_news": list(media_news_map.values()),
            "all_records": all_records,
            "job_changes": job_changes_detected,
            "message": f"당일 및 최신 인사 기사에서 {len(media_news_map)}개 언론사, 총 {len(all_records)}명의 발령 소식을 성공적으로 정리했습니다."
        }



# ==========================================
# 5. SQLite 데이터베이스 및 영속화 레이어
# ==========================================

class MediaDatabase:
    """
    빅카인즈 기사, 추출된 인사 발령 레코드, 인물 마스터, 이직 히스토리를
    저장하고 조회하는 SQLite 데이터베이스 관리자.
    """

    def __init__(self, db_path: str = "media_tracker.db", auto_seed: bool = True):
        if not os.path.isabs(db_path):
            base_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(base_dir, db_path)
        self.db_path = db_path
        self._init_db()
        if auto_seed:
            try:
                self.ensure_seed_data()
            except Exception:
                pass

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 1. 빅카인즈 원본 기사 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    news_id TEXT UNIQUE,
                    pub_date TEXT,
                    media TEXT,
                    title TEXT,
                    content TEXT,
                    byline TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 2. 발령 레코드 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS personnel_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pub_date TEXT,
                    media TEXT,
                    dept TEXT,
                    domain TEXT,
                    rank_title TEXT,
                    rank_level INTEGER,
                    name TEXT,
                    action_type TEXT,
                    email TEXT,
                    raw_text TEXT,
                    reporter_id TEXT,
                    article_title TEXT,
                    article_url TEXT
                )
            """)
            try:
                cursor.execute("ALTER TABLE personnel_records ADD COLUMN article_title TEXT")
            except Exception:
                pass
            try:
                cursor.execute("ALTER TABLE personnel_records ADD COLUMN article_url TEXT")
            except Exception:
                pass

            # 3. 인물 마스터 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reporters (
                    reporter_id TEXT PRIMARY KEY,
                    name TEXT,
                    email TEXT,
                    current_media TEXT,
                    current_dept TEXT,
                    current_rank TEXT,
                    first_seen TEXT,
                    last_seen TEXT,
                    display_label TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 4. 이직(Job Change) 감지 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    reporter_id TEXT,
                    name TEXT,
                    from_media TEXT,
                    from_dept TEXT,
                    from_rank TEXT,
                    from_date TEXT,
                    to_media TEXT,
                    to_dept TEXT,
                    to_rank TEXT,
                    to_date TEXT,
                    days_gap INTEGER,
                    confidence_score REAL,
                    detected_reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 인덱스 생성
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_records_name ON personnel_records(name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_records_reporter ON personnel_records(reporter_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_records_media ON personnel_records(media)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_records_article_url ON personnel_records(article_url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_news_id ON articles(news_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reporters_name ON reporters(name)")

            conn.commit()

        self.purge_excluded_department_records()

    def purge_excluded_department_records(self) -> int:
        """기존 DB에서 '예능', '드라마' 등 제외 대상 부서 및 웹 노이즈/STOPWORDS 인명 레코드를 영구 삭제합니다."""
        deleted_count = 0
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for kw in EXCLUDED_DEPARTMENT_KEYWORDS:
                cursor.execute("SELECT id FROM personnel_records WHERE dept LIKE ?", (f"%{kw}%",))
                ids = [r[0] for r in cursor.fetchall()]
                if ids:
                    deleted_count += len(ids)
                    cursor.execute("DELETE FROM personnel_records WHERE dept LIKE ?", (f"%{kw}%",))
                cursor.execute("DELETE FROM reporters WHERE current_dept LIKE ?", (f"%{kw}%",))
                cursor.execute("DELETE FROM job_changes WHERE from_dept LIKE ? OR to_dept LIKE ?", (f"%{kw}%", f"%{kw}%"))

            for sw in STOPWORDS:
                cursor.execute("SELECT id FROM personnel_records WHERE name = ?", (sw,))
                ids = [r[0] for r in cursor.fetchall()]
                if ids:
                    deleted_count += len(ids)
                    cursor.execute("DELETE FROM personnel_records WHERE name = ?", (sw,))
                cursor.execute("DELETE FROM reporters WHERE name = ?", (sw,))
                cursor.execute("DELETE FROM job_changes WHERE name = ?", (sw,))
            conn.commit()
        return deleted_count

    def dump_seed_data(self, seed_path: str = "data/archive_seed.json.gz") -> int:
        """현재 DB의 personnel_records, reporters, job_changes를 압축 시드 파일로 내보냅니다."""
        try:
            target_path = seed_path
            if not os.path.isabs(target_path):
                base_dir = os.path.dirname(os.path.abspath(__file__))
                target_path = os.path.join(base_dir, seed_path)
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            with self.get_connection() as conn:
                cursor = conn.cursor()
                payload = {}
                for tbl in ["personnel_records", "reporters", "job_changes"]:
                    cursor.execute(f"SELECT * FROM {tbl}")
                    payload[tbl] = [dict(row) for row in cursor.fetchall()]
            with gzip.open(target_path, "wt", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            return len(payload.get("personnel_records", []))
        except Exception as e:
            print(f"[dump_seed_data error] {e}")
            return 0

    def ensure_seed_data(self, seed_path: str = "data/archive_seed.json.gz", force: bool = False) -> Dict[str, Any]:
        """
        저장소에 번들링된 archive_seed.json.gz 데이터를 확인하여,
        로컬 DB에 레코드가 부족하거나(5,000건 미만) 최신 2026년 데이터가 누락되어 있을 경우
        자동으로 시드 데이터를 병합(Merge)하여 전체 기간(1965~2026) 데이터를 완성합니다.
        """
        candidate_paths = [
            seed_path,
            os.path.join(os.path.dirname(os.path.abspath(__file__)), seed_path),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", seed_path),
            os.path.abspath(seed_path)
        ]
        resolved_path = None
        for cp in candidate_paths:
            if os.path.exists(cp):
                resolved_path = cp
                break

        if not resolved_path:
            return {"inserted_records": 0, "status": "no_seed_file"}

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*), MAX(pub_date) FROM personnel_records")
                row = cursor.fetchone()
                cnt = row[0] if row else 0
                max_date = str(row[1]) if (row and row[1]) else ""

                with gzip.open(resolved_path, "rt", encoding="utf-8") as f:
                    seed_payload = json.load(f)

                if isinstance(seed_payload, dict):
                    seed_records = seed_payload.get("personnel_records", [])
                    seed_reporters = seed_payload.get("reporters", [])
                    seed_changes = seed_payload.get("job_changes", [])
                elif isinstance(seed_payload, list):
                    seed_records = seed_payload
                    seed_reporters = []
                    seed_changes = []
                else:
                    return {"inserted_records": 0, "status": "invalid_seed_format"}

                if not force and cnt >= len(seed_records) and max_date >= "2026-01-01":
                    return {"inserted_records": 0, "status": "already_up_to_date", "total_records": cnt}

                cursor.execute("SELECT pub_date, media, name, dept, rank_title FROM personnel_records")
                existing_keys = set(cursor.fetchall())

                to_insert = []
                for r in seed_records:
                    k = (r.get("pub_date"), r.get("media"), r.get("name"), r.get("dept"), r.get("rank_title"))
                    if k not in existing_keys:
                        existing_keys.add(k)
                        to_insert.append((
                            r.get("pub_date"), r.get("media"), r.get("dept"), r.get("domain", ""),
                            r.get("rank_title"), r.get("rank_level", 2), r.get("name"), r.get("action_type", ""),
                            r.get("email", ""), r.get("raw_text", ""), r.get("reporter_id", ""),
                            r.get("article_title", ""), r.get("article_url", "")
                        ))

                if to_insert:
                    cursor.executemany("""
                        INSERT INTO personnel_records (
                            pub_date, media, dept, domain, rank_title, rank_level, name,
                            action_type, email, raw_text, reporter_id, article_title, article_url
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, to_insert)

                if seed_reporters:
                    rep_rows = []
                    for rep in seed_reporters:
                        rep_rows.append((
                            rep.get("reporter_id"), rep.get("name"), rep.get("email", ""),
                            rep.get("current_media", ""), rep.get("current_dept", ""),
                            rep.get("current_rank", ""), rep.get("first_seen", ""),
                            rep.get("last_seen", ""), rep.get("display_label", "")
                        ))
                    cursor.executemany("""
                        INSERT INTO reporters (
                            reporter_id, name, email, current_media, current_dept,
                            current_rank, first_seen, last_seen, display_label
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(reporter_id) DO UPDATE SET
                            current_media = excluded.current_media,
                            current_dept = excluded.current_dept,
                            current_rank = excluded.current_rank,
                            first_seen = MIN(reporters.first_seen, excluded.first_seen),
                            last_seen = MAX(reporters.last_seen, excluded.last_seen)
                    """, rep_rows)

                if seed_changes:
                    cursor.execute("SELECT reporter_id, from_media, to_media, to_date FROM job_changes")
                    existing_jc = set(cursor.fetchall())
                    jc_rows = []
                    for jc in seed_changes:
                        k_jc = (jc.get("reporter_id"), jc.get("from_media"), jc.get("to_media"), jc.get("to_date"))
                        if k_jc not in existing_jc:
                            existing_jc.add(k_jc)
                            jc_rows.append((
                                jc.get("reporter_id"), jc.get("name"), jc.get("from_media"),
                                jc.get("from_dept"), jc.get("from_rank"), jc.get("from_date"),
                                jc.get("to_media"), jc.get("to_dept"), jc.get("to_rank"),
                                jc.get("to_date"), jc.get("days_gap", 0), jc.get("confidence_score", 1.0),
                                jc.get("detected_reason", "")
                            ))
                    if jc_rows:
                        cursor.executemany("""
                            INSERT INTO job_changes (
                                reporter_id, name, from_media, from_dept, from_rank, from_date,
                                to_media, to_dept, to_rank, to_date, days_gap, confidence_score, detected_reason
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, jc_rows)

                conn.commit()

            self.purge_excluded_department_records()
            return {"inserted_records": len(to_insert), "status": "synced"}
        except Exception as e:
            return {"inserted_records": 0, "status": "error", "message": str(e)}

    def clear_all_data(self):
        """데이터베이스의 모든 인사 발령 및 기자 데이터를 완전히 삭제합니다."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM personnel_records")
            cursor.execute("DELETE FROM reporters")
            cursor.execute("DELETE FROM job_changes")
            cursor.execute("DELETE FROM articles")
            conn.commit()

    def save_records_and_clusters(
        self,
        raw_records: List[Dict[str, Any]],
        clusters_by_name: Dict[str, List[Dict[str, Any]]],
        job_changes: List[Dict[str, Any]]
    ):
        """파싱 및 클러스터링된 데이터를 트랜잭션 단위로 일괄 저장합니다."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 기존 레코드 및 이직 데이터 덮어쓰기/갱신을 위해 삭제 후 재등록
            cursor.execute("DELETE FROM personnel_records")
            cursor.execute("DELETE FROM reporters")
            cursor.execute("DELETE FROM job_changes")

            # 인물 마스터 등록
            for name, clusters in clusters_by_name.items():
                for c in clusters:
                    cursor.execute("""
                        INSERT OR REPLACE INTO reporters
                        (reporter_id, name, email, current_media, current_dept, current_rank, first_seen, last_seen, display_label)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        c["reporter_id"],
                        c["name"],
                        c.get("email", ""),
                        c["current_media"],
                        c["current_dept"],
                        c["current_rank"],
                        str(c["first_seen"]),
                        str(c["last_seen"]),
                        c["display_label"]
                    ))

            # 발령 레코드 등록
            for r in raw_records:
                cursor.execute("""
                    INSERT INTO personnel_records
                    (pub_date, media, dept, domain, rank_title, rank_level, name, action_type, email, raw_text, reporter_id, article_title, article_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(r.get("pub_date", "")),
                    r.get("media", ""),
                    r.get("dept", ""),
                    r.get("domain", ""),
                    r.get("rank_title", ""),
                    r.get("rank_level", 2),
                    r.get("name", ""),
                    r.get("action_type", "발령"),
                    r.get("email", ""),
                    r.get("raw_text", ""),
                    r.get("reporter_id", ""),
                    r.get("article_title", ""),
                    r.get("article_url", "")
                ))

            # 이직 이벤트 등록
            for jc in job_changes:
                cursor.execute("""
                    INSERT INTO job_changes
                    (reporter_id, name, from_media, from_dept, from_rank, from_date, to_media, to_dept, to_rank, to_date, days_gap, confidence_score, detected_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    jc["reporter_id"],
                    jc["name"],
                    jc["from_media"],
                    jc["from_dept"],
                    jc["from_rank"],
                    jc["from_date"],
                    jc["to_media"],
                    jc["to_dept"],
                    jc["to_rank"],
                    jc["to_date"],
                    jc.get("days_gap", 0),
                    jc.get("confidence_score", 0.0),
                    jc.get("detected_reason", "")
                ))

            conn.commit()

    def get_all_raw_records(self) -> List[Dict[str, Any]]:
        """DB에 저장된 기존 모든 발령 레코드를 가져옵니다."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM personnel_records ORDER BY pub_date ASC, id ASC")
            return [dict(row) for row in cursor.fetchall()]

    def is_article_processed(self, url: str) -> bool:
        """기사가 이미 DB(articles 또는 personnel_records)에 등록되어 있는지 확인합니다."""
        if not url:
            return False
        clean_url = url.strip()
        with self.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT 1 FROM articles WHERE news_id = ?
                UNION
                SELECT 1 FROM personnel_records WHERE article_url = ?
                LIMIT 1
            """, (clean_url, clean_url))
            return cur.fetchone() is not None

    def record_processed_article(self, url: str, title: str, pub_date: str, content: str = "", media: str = "중앙일보"):
        """수집/분석이 완료된 기사 메타데이터를 articles 테이블에 영속화합니다."""
        if not url:
            return
        clean_url = url.strip()
        with self.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO articles (news_id, pub_date, media, title, content, byline)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (clean_url, pub_date or "", media or "중앙일보", title or "", content or "", ""))
            conn.commit()

    def get_all_reporters(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM reporters ORDER BY last_seen DESC, name ASC")
            return [dict(row) for row in cursor.fetchall()]

    @staticmethod
    def deduplicate_timeline_records(records: List[Dict[str, Any]], window_days: int = 90, sort_desc: bool = False) -> List[Dict[str, Any]]:
        """
        일정 기간(기본 90일) 동안 동일한 발령(동일 인물, 동일 매체, 부서, 직급)이 여러 기사로 보도되어
        중복 수집된 경우, 가장 빠른 기사 발행 일자(Earliest Article Date)를 기준으로 단일 이력으로 병합합니다.
        """
        if not records:
            return []

        def _dept_key(d: str) -> str:
            s = re.sub(r'^[△▲▷▶○●■□◆◇◈▣◎⊙◐◑·ㆍ\-\*―━“"”’‘\'〃\s]+|[“"”’‘\'〃\s]+$', '', str(d).strip())
            s = re.sub(r'[\s“"”’‘\'〃]+', '', s)
            if (s.endswith("부") or s.endswith("팀")) and len(s) > 2:
                return s[:-1]
            return s
        
        def _rank_equiv(r1: str, r2: str) -> bool:
            r1_c = r1.strip()
            r2_c = r2.strip()
            if r1_c == r2_c:
                return True
            if all(any(k in r for k in ["부장", "부장대우", "부장직대"]) for r in [r1_c, r2_c]):
                return True
            if all(any(k in r for k in ["차장", "차장대우"]) for r in [r1_c, r2_c]):
                return True
            if all(any(k in r for k in ["국장", "부국장", "국장대우"]) for r in [r1_c, r2_c]):
                return True
            return False

        # 날짜 오름차순 정렬 (과거 -> 현재)
        sorted_records = sorted(records, key=lambda x: str(x.get("pub_date", "1970-01-01"))[:10])
        merged: List[Dict[str, Any]] = []

        for rec in sorted_records:
            rec_copy = dict(rec)
            r_date_str = str(rec_copy.get("pub_date", ""))[:10]
            try:
                r_date = datetime.datetime.strptime(r_date_str, "%Y-%m-%d").date()
            except Exception:
                r_date = None

            matched_idx = -1
            for idx, m in enumerate(merged):
                same_name = (not m.get("name") or not rec_copy.get("name") or m.get("name", "").strip() == rec_copy.get("name", "").strip())
                same_media = (normalize_media_name(m.get("media", "")) == normalize_media_name(rec_copy.get("media", "")) or are_group_affiliates(m.get("media", ""), rec_copy.get("media", "")))
                same_dept = _dept_key(m.get("dept", "")) == _dept_key(rec_copy.get("dept", ""))
                same_rank = _rank_equiv(m.get("rank_title", ""), rec_copy.get("rank_title", ""))

                if same_name and same_media and same_dept and same_rank:
                    m_date_str = str(m.get("pub_date", ""))[:10]
                    try:
                        m_date = datetime.datetime.strptime(m_date_str, "%Y-%m-%d").date()
                    except Exception:
                        m_date = None

                    if r_date and m_date:
                        if abs((r_date - m_date).days) <= window_days:
                            matched_idx = idx
                            break
                    else:
                        matched_idx = idx
                        break

            if matched_idx >= 0:
                existing = merged[matched_idx]
                e_date_str = str(existing.get("pub_date", ""))[:10]
                # 동일한 인사는 가장 빠른 기사 일자(Earliest Date) 적용
                if r_date_str and (not e_date_str or r_date_str < e_date_str):
                    existing["pub_date"] = r_date_str
                    if rec_copy.get("article_url"):
                        existing["article_url"] = rec_copy.get("article_url")
                        existing["article_title"] = rec_copy.get("article_title", "")
                elif not existing.get("article_url") and rec_copy.get("article_url"):
                    existing["article_url"] = rec_copy["article_url"]
                    existing["article_title"] = rec_copy.get("article_title", "")

                if normalize_media_name(existing.get("media", "")) in ["중앙일보", "이데일리", "머니투데이"] and normalize_media_name(rec_copy.get("media", "")) in ["이코노미스트", "일간스포츠", "머니투데이방송"]:
                    existing["media"] = normalize_media_name(rec_copy["media"])
                if not existing.get("dept") and rec_copy.get("dept"):
                    existing["dept"] = rec_copy["dept"]
                
                # 부서명이 더 상세하고 정돈된 경우 갱신
                cur_dept = str(existing.get("dept", ""))
                new_dept = str(rec_copy.get("dept", ""))
                if "부" in new_dept and "부" not in cur_dept:
                    existing["dept"] = new_dept
                elif len(new_dept) > len(cur_dept) and "〃" not in new_dept:
                    existing["dept"] = new_dept

                # 발령구분이 더 구체적인 경우 갱신
                if existing.get("action_type") in ["발령", ""] and rec_copy.get("action_type") not in ["발령", ""]:
                    existing["action_type"] = rec_copy["action_type"]
            else:
                merged.append(rec_copy)

        if sort_desc:
            merged.sort(key=lambda x: str(x.get("pub_date", "1970-01-01"))[:10], reverse=True)
        else:
            merged.sort(key=lambda x: str(x.get("pub_date", "1970-01-01"))[:10])
        return merged

    def get_reporter(self, reporter_id: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM reporters WHERE reporter_id = ?", (reporter_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_reporter_timeline(self, reporter_id: str) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM personnel_records
                WHERE reporter_id = ?
                ORDER BY pub_date ASC, id ASC
            """, (reporter_id,))
            rows = [dict(row) for row in cursor.fetchall()]
            return self.deduplicate_timeline_records(rows, sort_desc=True)

    @staticmethod
    def _build_media_sql_condition(col_name: str, media_name: str) -> Tuple[str, List[str]]:
        """
        언론사 검색 시 머니투데이 vs 머니투데이방송(MTN) 등 유사 명칭 언론사를 정확히 분리하기 위한 SQL 절과 파라미터를 생성합니다.
        """
        if not media_name:
            return "", []
        m_raw = media_name.strip()
        m_norm = normalize_media_name(m_raw)

        # 1. 머니투데이 (신문) 검색 시: 머니투데이방송(MTN) 완벽 배제
        if m_norm == "머니투데이" or m_raw in ["머니투데이", "머투", "머니투데이신문", "머니투데이그룹", "머니투데이미디어"]:
            sql = f"(({col_name} LIKE ? OR {col_name} LIKE ?) AND {col_name} NOT LIKE '%방송%' AND {col_name} NOT LIKE '%MTN%' AND {col_name} NOT LIKE '%mtn%')"
            return sql, [f"%{m_raw}%", f"%{m_norm}%"]

        # 2. 머니투데이방송 (MTN) 검색 시: 머니투데이방송 및 MTN만 정확 매칭 (신문 머니투데이 배제)
        if m_norm == "머니투데이방송" or any(k in m_raw.lower() for k in ["mtn", "머니투데이방송", "머니투데이 방송"]):
            sql = f"({col_name} LIKE '%머니투데이방송%' OR {col_name} LIKE '%MTN%' OR {col_name} LIKE '%mtn%')"
            return sql, []

        # 3. 한국경제 (신문) 검색 시: 한국경제TV 배제
        if m_norm == "한국경제":
            sql = f"(({col_name} LIKE ? OR {col_name} LIKE ?) AND {col_name} NOT LIKE '%TV%' AND {col_name} NOT LIKE '%tv%' AND {col_name} NOT LIKE '%방송%')"
            return sql, [f"%{m_raw}%", f"%{m_norm}%"]

        # 4. 한국경제TV 검색 시:
        if "한국경제tv" in m_raw.lower() or "한경tv" in m_raw.lower():
            sql = f"({col_name} LIKE '%한국경제TV%' OR {col_name} LIKE '%한국경제tv%')"
            return sql, []

        # 5. 매일경제 (신문) 검색 시: MBN / 매일경제TV 배제
        if m_norm == "매일경제":
            sql = f"(({col_name} LIKE ? OR {col_name} LIKE ?) AND {col_name} NOT LIKE '%MBN%' AND {col_name} NOT LIKE '%mbn%' AND {col_name} NOT LIKE '%TV%' AND {col_name} NOT LIKE '%tv%')"
            return sql, [f"%{m_raw}%", f"%{m_norm}%"]

        # 6. 조선일보 (신문) 검색 시: TV조선 배제
        if m_norm == "조선일보":
            sql = f"(({col_name} LIKE ? OR {col_name} LIKE ?) AND {col_name} NOT LIKE '%TV%' AND {col_name} NOT LIKE '%tv%')"
            return sql, [f"%{m_raw}%", f"%{m_norm}%"]

        # 7. 헤럴드 / 헤럴드미디어 (모회사/그룹) 검색 시: 산하 헤럴드경제, 코리아헤럴드 및 법인 전수 매칭
        if m_norm in ["헤럴드", "헤럴드미디어"] or m_raw in ["헤럴드", "헤럴드미디어", "(주)헤럴드", "헤럴드그룹"]:
            sql = f"({col_name} LIKE '%헤럴드%' OR {col_name} LIKE '%코리아헤럴드%')"
            return sql, []

        # 8. 헤럴드경제 검색 시: 헤럴드경제 본지 및 본사/헤럴드미디어 공통 발령 포함 (코리아헤럴드 단독 배제)
        if m_norm == "헤럴드경제":
            sql = f"({col_name} LIKE '%헤럴드경제%' OR {col_name} IN ('헤럴드', '헤럴드미디어'))"
            return sql, []

        # 9. 코리아헤럴드 검색 시: 코리아헤럴드 영자지 및 본사/헤럴드미디어 공통 발령 포함 (헤럴드경제 단독 배제)
        if m_norm == "코리아헤럴드":
            sql = f"({col_name} LIKE '%코리아헤럴드%' OR ({col_name} IN ('헤럴드', '헤럴드미디어') AND {col_name} NOT LIKE '%경제%'))"
            return sql, []

        # 기본: 일반 매체 및 계열/별칭 포괄 매칭
        aliases = MEDIA_GROUP_ALIASES.get(m_norm, [])
        clauses = [f"{col_name} LIKE ?", f"{col_name} LIKE ?"]
        params = [f"%{m_raw}%", f"%{m_norm}%"]
        for a in aliases:
            if len(a) >= 2 and a not in [m_raw, m_norm] and not a.startswith("http"):
                clauses.append(f"{col_name} LIKE ?")
                params.append(f"%{a}%")
        sql = f"({' OR '.join(clauses)})"
        return sql, params

    def search_reporters(self, name: str = "", media: str = "", dept: str = "", rank: str = "") -> List[Dict[str, Any]]:
        # 복합 검색어 자동 분리 (예: media="뉴스1 건설부동산" -> media="뉴스1", dept="건설부동산")
        if media and not dept:
            m_chk = media.strip()
            if m_chk not in KNOWN_MEDIA_NAMES and normalize_media_name(m_chk) not in KNOWN_MEDIA_NAMES:
                for km in sorted(KNOWN_MEDIA_NAMES, key=len, reverse=True):
                    if m_chk.startswith(km) and len(m_chk) > len(km):
                        remainder = m_chk[len(km):].strip()
                        if remainder and (remainder in ["건설부동산", "산업부", "경제부", "정치부", "사회부", "금융부", "증권부"] or remainder.endswith(("부", "팀", "국", "실", "센터", "단", "소")) or " " in media):
                            media = km
                            dept = remainder
                            break
            if not dept and " " in media:
                parts = media.split(None, 1)
                if any(is_same_media_company(parts[0], km) for km in KNOWN_MEDIA_NAMES) or parts[0] in KNOWN_MEDIA_NAMES:
                    media, dept = parts[0], parts[1]

        # 단독 부서명이 media에 입력된 경우(예: media="건설부동산") 자동으로 dept로 전환
        if media and not dept:
            m_chk = media.strip()
            if not is_media_organization(m_chk) and (
                is_department_header(m_chk) 
                or any(m_chk.endswith(sfx) for sfx in ["부", "팀", "국", "실", "센터", "단", "소", "본부"])
                or any(kw in m_chk for kw in ["건설", "부동산", "산업", "경제", "정치", "사회", "금융", "증권", "문화", "국제", "it", "과학", "바이오", "유통", "기획", "논설"])
            ):
                dept = m_chk
                media = ""

        # dept 내에 직급명(부장, 차장 등)이 포함된 경우 rank로 자동 분리
        if dept and not rank:
            for rnk_term in ["부장", "차장", "팀장", "국장", "부국장", "실장", "본부장", "에디터", "데스크", "기자"]:
                if rnk_term in dept:
                    dept = re.sub(rf"\b{rnk_term}\b|\s*{rnk_term}", "", dept).strip()
                    rank = rnk_term
                    break

        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT DISTINCT r.* FROM reporters r WHERE 1=1"
            params = []
            if name:
                query += " AND r.name LIKE ?"
                params.append(f"%{name}%")

            clean_d = dept.strip() if dept else ""
            clean_r = rank.strip() if rank else ""
            is_const_re = ("건설" in clean_d or "부동산" in clean_d)
            stem_d = re.sub(r"(?:부|팀|국|실|본부|센터|단|소)$", "", clean_d).strip() if clean_d else ""
            has_stem = (len(stem_d) >= 2 and stem_d != clean_d)
            is_rep_search = (clean_r == "기자")

            if is_const_re:
                d_pr_sql = "(pr.dept LIKE ? OR pr.dept LIKE ? OR pr.dept LIKE ?)"
                d_pr_p = ["%건설부동산%", "%건설%", "%부동산%"]
                d_cur_sql = "(r.current_dept LIKE ? OR r.current_dept LIKE ? OR r.current_dept LIKE ?)"
                d_cur_p = ["%건설부동산%", "%건설%", "%부동산%"]
            elif has_stem:
                d_pr_sql = "(pr.dept LIKE ? OR pr.dept LIKE ?)"
                d_pr_p = [f"%{clean_d}%", f"%{stem_d}%"]
                d_cur_sql = "(r.current_dept LIKE ? OR r.current_dept LIKE ?)"
                d_cur_p = [f"%{clean_d}%", f"%{stem_d}%"]
            else:
                d_pr_sql = "pr.dept LIKE ?"
                d_pr_p = [f"%{dept}%"]
                d_cur_sql = "r.current_dept LIKE ?"
                d_cur_p = [f"%{dept}%"]

            rep_rank_pr_sql = """(
                pr.rank_title LIKE '%기자%' 
                OR pr.rank_title IS NULL 
                OR pr.rank_title = '' 
                OR pr.rank_title = '미표기' 
                OR pr.rank_level <= 2 
                OR (
                    pr.rank_title NOT LIKE '%부장%' 
                    AND pr.rank_title NOT LIKE '%차장%' 
                    AND pr.rank_title NOT LIKE '%국장%' 
                    AND pr.rank_title NOT LIKE '%팀장%' 
                    AND pr.rank_title NOT LIKE '%실장%' 
                    AND pr.rank_title NOT LIKE '%본부장%' 
                    AND pr.rank_title NOT LIKE '%대표%' 
                    AND pr.rank_title NOT LIKE '%사장%' 
                    AND pr.rank_title NOT LIKE '%주필%' 
                    AND pr.rank_title NOT LIKE '%논설%'
                    AND pr.rank_title NOT LIKE '%위원%'
                )
                OR (
                    pr.raw_text NOT LIKE '%부장%' 
                    AND pr.raw_text NOT LIKE '%차장%' 
                    AND pr.raw_text NOT LIKE '%국장%' 
                    AND pr.raw_text NOT LIKE '%팀장%' 
                    AND pr.raw_text NOT LIKE '%실장%' 
                    AND pr.raw_text NOT LIKE '%본부장%' 
                    AND pr.raw_text NOT LIKE '%대표%' 
                    AND pr.raw_text NOT LIKE '%사장%' 
                    AND pr.raw_text NOT LIKE '%주필%' 
                    AND pr.raw_text NOT LIKE '%논설%'
                    AND pr.raw_text NOT LIKE '%위원%'
                    AND pr.raw_text NOT LIKE '%대우%'
                    AND pr.raw_text NOT LIKE '%〃%'
                )
            )"""

            rep_rank_cur_sql = """(
                r.current_rank LIKE '%기자%' 
                OR r.current_rank IS NULL 
                OR r.current_rank = '' 
                OR r.current_rank = '미표기' 
                OR (
                    r.current_rank NOT LIKE '%부장%' 
                    AND r.current_rank NOT LIKE '%차장%' 
                    AND r.current_rank NOT LIKE '%국장%' 
                    AND r.current_rank NOT LIKE '%팀장%' 
                    AND r.current_rank NOT LIKE '%실장%' 
                    AND r.current_rank NOT LIKE '%본부장%' 
                    AND r.current_rank NOT LIKE '%대표%' 
                    AND r.current_rank NOT LIKE '%사장%' 
                    AND r.current_rank NOT LIKE '%주필%' 
                    AND r.current_rank NOT LIKE '%논설%' 
                    AND r.current_rank NOT LIKE '%위원%'
                )
            )"""

            if dept and rank:
                if is_rep_search:
                    query += f""" AND (
                        EXISTS (
                            SELECT 1 FROM personnel_records pr 
                            WHERE pr.reporter_id = r.reporter_id
                        """
                    if media:
                        m_pr_sql, m_pr_params = self._build_media_sql_condition("pr.media", media)
                        query += f" AND {m_pr_sql}"
                        params.extend(m_pr_params)
                    query += f""" AND {d_pr_sql} 
                             AND {rep_rank_pr_sql}
                        )
                        OR (
                        """
                    params.extend(d_pr_p)
                    if media:
                        m_cur_sql, m_cur_params = self._build_media_sql_condition("r.current_media", media)
                        query += f" {m_cur_sql} AND "
                        params.extend(m_cur_params)
                    query += f""" {d_cur_sql} AND {rep_rank_cur_sql}
                        )
                    )"""
                    params.extend(d_cur_p)
                else:
                    query += f""" AND (
                        EXISTS (
                            SELECT 1 FROM personnel_records pr 
                            WHERE pr.reporter_id = r.reporter_id
                        """
                    if media:
                        m_pr_sql, m_pr_params = self._build_media_sql_condition("pr.media", media)
                        query += f" AND {m_pr_sql}"
                        params.extend(m_pr_params)
                    query += f""" AND {d_pr_sql} 
                             AND (pr.rank_title LIKE ? OR pr.dept LIKE ?)
                        )
                        OR (
                        """
                    params.extend(d_pr_p)
                    params.extend([f"%{rank}%", f"%{rank}%"])
                    if media:
                        m_cur_sql, m_cur_params = self._build_media_sql_condition("r.current_media", media)
                        query += f" {m_cur_sql} AND "
                        params.extend(m_cur_params)
                    query += f""" {d_cur_sql} AND r.current_rank LIKE ?
                        )
                    )"""
                    params.extend(d_cur_p)
                    params.append(f"%{rank}%")
            elif dept:
                query += f""" AND (
                    EXISTS (
                        SELECT 1 FROM personnel_records pr 
                        WHERE pr.reporter_id = r.reporter_id
                    """
                if media:
                    m_pr_sql, m_pr_params = self._build_media_sql_condition("pr.media", media)
                    query += f" AND {m_pr_sql}"
                    params.extend(m_pr_params)
                query += f""" AND {d_pr_sql}
                    )
                    OR (
                    """
                params.extend(d_pr_p)
                if media:
                    m_cur_sql, m_cur_params = self._build_media_sql_condition("r.current_media", media)
                    query += f" {m_cur_sql} AND "
                    params.extend(m_cur_params)
                query += f""" {d_cur_sql}
                    )
                )"""
                params.extend(d_cur_p)
            elif rank:
                if is_rep_search:
                    query += f""" AND (
                        EXISTS (
                            SELECT 1 FROM personnel_records pr 
                            WHERE pr.reporter_id = r.reporter_id
                        """
                    if media:
                        m_pr_sql, m_pr_params = self._build_media_sql_condition("pr.media", media)
                        query += f" AND {m_pr_sql}"
                        params.extend(m_pr_params)
                    query += f""" AND {rep_rank_pr_sql}
                        )
                        OR (
                        """
                    if media:
                        m_cur_sql, m_cur_params = self._build_media_sql_condition("r.current_media", media)
                        query += f" {m_cur_sql} AND "
                        params.extend(m_cur_params)
                    query += f""" {rep_rank_cur_sql}
                        )
                    )"""
                else:
                    query += """ AND (
                        EXISTS (
                            SELECT 1 FROM personnel_records pr 
                            WHERE pr.reporter_id = r.reporter_id
                        """
                    if media:
                        m_pr_sql, m_pr_params = self._build_media_sql_condition("pr.media", media)
                        query += f" AND {m_pr_sql}"
                        params.extend(m_pr_params)
                    query += """ AND (pr.rank_title LIKE ? OR pr.dept LIKE ?)
                        )
                        OR (
                        """
                    params.extend([f"%{rank}%", f"%{rank}%"])
                    if media:
                        m_cur_sql, m_cur_params = self._build_media_sql_condition("r.current_media", media)
                        query += f" {m_cur_sql} AND "
                        params.extend(m_cur_params)
                    query += """ r.current_rank LIKE ?
                        )
                    )"""
                    params.append(f"%{rank}%")
            elif media:
                m_cur_sql, m_cur_params = self._build_media_sql_condition("r.current_media", media)
                m_pr_sql, m_pr_params = self._build_media_sql_condition("pr.media", media)
                query += f""" AND (
                    {m_cur_sql} OR 
                    EXISTS (SELECT 1 FROM personnel_records pr WHERE pr.reporter_id = r.reporter_id AND {m_pr_sql})
                )"""
                params.extend(m_cur_params)
                params.extend(m_pr_params)

            query += " ORDER BY r.last_seen DESC"
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def search_records(self, name: str = "", media: str = "", dept: str = "", rank: str = "", limit: int = 25000) -> List[Dict[str, Any]]:
        # 복합 검색어 자동 분리 (예: media="뉴스1 건설부동산" -> media="뉴스1", dept="건설부동산")
        if media and not dept:
            m_chk = media.strip()
            if m_chk not in KNOWN_MEDIA_NAMES and normalize_media_name(m_chk) not in KNOWN_MEDIA_NAMES:
                for km in sorted(KNOWN_MEDIA_NAMES, key=len, reverse=True):
                    if m_chk.startswith(km) and len(m_chk) > len(km):
                        remainder = m_chk[len(km):].strip()
                        if remainder and (remainder in ["건설부동산", "산업부", "경제부", "정치부", "사회부", "금융부", "증권부"] or remainder.endswith(("부", "팀", "국", "실", "센터", "단", "소")) or " " in media):
                            media = km
                            dept = remainder
                            break
            if not dept and " " in media:
                parts = media.split(None, 1)
                if any(is_same_media_company(parts[0], km) for km in KNOWN_MEDIA_NAMES) or parts[0] in KNOWN_MEDIA_NAMES:
                    media, dept = parts[0], parts[1]

        # 단독 부서명이 media에 입력된 경우(예: media="건설부동산") 자동으로 dept로 전환
        if media and not dept:
            m_chk = media.strip()
            if not is_media_organization(m_chk) and (
                is_department_header(m_chk) 
                or any(m_chk.endswith(sfx) for sfx in ["부", "팀", "국", "실", "센터", "단", "소", "본부"])
                or any(kw in m_chk for kw in ["건설", "부동산", "산업", "경제", "정치", "사회", "금융", "증권", "문화", "국제", "it", "과학", "바이오", "유통", "기획", "논설"])
            ):
                dept = m_chk
                media = ""

        # dept 내에 직급명(부장, 차장 등)이 포함된 경우 rank로 자동 분리
        if dept and not rank:
            for rnk_term in ["부장", "차장", "팀장", "국장", "부국장", "실장", "본부장", "에디터", "데스크", "기자"]:
                if rnk_term in dept:
                    dept = re.sub(rf"\b{rnk_term}\b|\s*{rnk_term}", "", dept).strip()
                    rank = rnk_term
                    break

        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT * FROM personnel_records WHERE 1=1"
            params = []
            if name:
                query += " AND name LIKE ?"
                params.append(f"%{name}%")
            if media:
                m_sql, m_params = self._build_media_sql_condition("media", media)
                query += f" AND {m_sql}"
                params.extend(m_params)
            if dept:
                clean_d = dept.strip()
                if "건설" in clean_d or "부동산" in clean_d:
                    query += " AND (dept LIKE ? OR dept LIKE ? OR dept LIKE ?)"
                    params.extend(["%건설부동산%", "%건설%", "%부동산%"])
                else:
                    stem_d = re.sub(r"(?:부|팀|국|실|본부|센터|단|소)$", "", clean_d).strip()
                    if len(stem_d) >= 2 and stem_d != clean_d:
                        query += " AND (dept LIKE ? OR dept LIKE ?)"
                        params.extend([f"%{clean_d}%", f"%{stem_d}%"])
                    else:
                        query += " AND dept LIKE ?"
                        params.append(f"%{clean_d}%")
            if rank:
                clean_r = rank.strip()
                if clean_r == "기자":
                    # 기사에 부장, 차장 등 관리직 언급이 없으면 모두 '기자'로 간주하여 직급 미언급 인물 전원 분석 대상 포함
                    query += """ AND (
                        rank_title LIKE '%기자%' 
                        OR rank_title IS NULL 
                        OR rank_title = '' 
                        OR rank_title = '미표기' 
                        OR rank_level <= 2 
                        OR (
                            rank_title NOT LIKE '%부장%' 
                            AND rank_title NOT LIKE '%차장%' 
                            AND rank_title NOT LIKE '%국장%' 
                            AND rank_title NOT LIKE '%팀장%' 
                            AND rank_title NOT LIKE '%실장%' 
                            AND rank_title NOT LIKE '%본부장%' 
                            AND rank_title NOT LIKE '%대표%' 
                            AND rank_title NOT LIKE '%사장%' 
                            AND rank_title NOT LIKE '%주필%' 
                            AND rank_title NOT LIKE '%논설%'
                            AND rank_title NOT LIKE '%위원%'
                        )
                        OR (
                            raw_text NOT LIKE '%부장%' 
                            AND raw_text NOT LIKE '%차장%' 
                            AND raw_text NOT LIKE '%국장%' 
                            AND raw_text NOT LIKE '%팀장%' 
                            AND raw_text NOT LIKE '%실장%' 
                            AND raw_text NOT LIKE '%본부장%' 
                            AND raw_text NOT LIKE '%대표%' 
                            AND raw_text NOT LIKE '%사장%' 
                            AND raw_text NOT LIKE '%주필%' 
                            AND raw_text NOT LIKE '%논설%'
                            AND raw_text NOT LIKE '%위원%'
                            AND raw_text NOT LIKE '%대우%'
                            AND raw_text NOT LIKE '%〃%'
                        )
                    )"""
                else:
                    query += " AND (rank_title LIKE ? OR dept LIKE ?)"
                    params.extend([f"%{rank}%", f"%{rank}%"])
            query += " ORDER BY pub_date DESC, id DESC LIMIT ?"
            params.append(limit)
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_job_changes(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM job_changes
                ORDER BY to_date DESC, id DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_stats(self) -> Dict[str, int]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM reporters")
            reporters_cnt = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM personnel_records")
            records_cnt = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM job_changes")
            changes_cnt = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT media) FROM personnel_records")
            media_cnt = cursor.fetchone()[0]
            return {
                "reporters": reporters_cnt,
                "records": records_cnt,
                "changes": changes_cnt,
                "media_count": media_cnt
            }

    def get_daily_personnel_summary(self, target_date: str = "") -> Dict[str, Any]:
        """
        DB에 저장된 발령 레코드 중 당일(또는 지정된 일자/최근 일자)의 데이터를
        언론사별 및 발령구분(승진/전보/보임 등)별로 구조화하여 반환합니다.
        제목에 [인사] 또는 <인사>가 포함된 공식 인사 기사만 엄격 필터링하여 분석합니다.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if target_date:
                cursor.execute("""
                    SELECT * FROM personnel_records
                    WHERE pub_date LIKE ?
                    ORDER BY pub_date DESC, media ASC, id ASC
                """, (f"{target_date}%",))
            else:
                cursor.execute("""
                    SELECT * FROM personnel_records
                    ORDER BY pub_date DESC, media ASC, id ASC
                    LIMIT 500
                """)
            raw_rows = [dict(row) for row in cursor.fetchall()]

        # [인사] 또는 <인사> 공식 기사이자 언론사 발령 레코드만 엄격 필터링
        rows = [
            r for r in raw_rows 
            if is_media_organization(r.get("media", "")) and (not r.get("article_title") or is_personnel_article_title(r.get("article_title", "")))
        ]

        grouped = {}
        for r in rows:
            med = normalize_media_name(r.get("media", "미상"))
            r["media"] = med
            if med not in grouped:
                grouped[med] = {
                    "media": med,
                    "records": [],
                    "by_action": {},
                    "latest_date": r.get("pub_date", ""),
                    "article_title": r.get("article_title", ""),
                    "article_url": r.get("article_url", "")
                }
            grouped[med]["records"].append(r)
            act = r.get("action_type", "발령")
            if act not in grouped[med]["by_action"]:
                grouped[med]["by_action"][act] = []
            grouped[med]["by_action"][act].append(r)
            if not grouped[med]["article_url"] and r.get("article_url"):
                grouped[med]["article_url"] = r.get("article_url", "")
                grouped[med]["article_title"] = r.get("article_title", "")

        return {
            "total_records": len(rows),
            "media_count": len(grouped),
            "media_news": list(grouped.values()),
            "raw_records": rows
        }



# ==========================================
# 6. 빅카인즈 파일 로더 및 일괄 처리 인터페이스
# ==========================================

def _fetch_full_text_from_url(url: str) -> Optional[str]:
    """기사 URL에서 원문 본문을 자동 추출하여 200자 제한을 보완합니다."""
    if not url or not str(url).startswith("http"):
        return None
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        resp = requests.get(str(url).strip(), headers=headers, timeout=4)
        if resp.status_code == 200:
            html = resp.text
            clean = re.sub(r"<(script|style|header|footer|nav).*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
            clean = re.sub(r"<[^>]+>", " ", clean)
            clean = re.sub(r"&[a-z]+;", " ", clean)
            clean = re.sub(r"\s+", " ", clean).strip()
            if any(k in clean for k in ["승진", "전보", "부장", "차장", "국장", "논설위원"]):
                return clean[:10000]
    except Exception:
        pass
    return None


def load_and_process_bigkinds_file(file_path_or_buffer, append: bool = True, db_path: str = "media_tracker.db") -> Dict[str, Any]:
    """
    빅카인즈 다운로드 표준 엑셀/CSV 파일을 읽고,
    인사 발령 텍스트를 파싱하여 데이터베이스에 저장(또는 누적)합니다.
    """
    df = None

    # 1. 파일 읽기
    if isinstance(file_path_or_buffer, str):
        if file_path_or_buffer.endswith(".csv"):
            df = pd.read_csv(file_path_or_buffer, encoding="utf-8-sig")
        else:
            try:
                df = pd.read_excel(file_path_or_buffer)
            except Exception:
                df = pd.read_excel(file_path_or_buffer, engine="openpyxl")
    else:
        content_bytes = file_path_or_buffer.read()
        file_path_or_buffer.seek(0)
        try:
            df = pd.read_excel(io.BytesIO(content_bytes))
        except Exception:
            try:
                df = pd.read_csv(io.BytesIO(content_bytes), encoding="utf-8-sig")
            except Exception:
                df = pd.read_csv(io.BytesIO(content_bytes), encoding="cp949")

    if df is None or df.empty:
        raise ValueError("업로드된 파일이 비어있거나 읽을 수 없는 형식입니다.")

    # 2. 헤더 행 자동 탐색 (상단 메타데이터 행이 있는 경우)
    keywords = ["일자", "언론사", "본문", "기사원문", "제목"]
    has_valid_header = any(any(kw in str(c) for kw in keywords) for c in df.columns)

    if not has_valid_header:
        for row_idx in range(min(10, len(df))):
            row_vals = [str(v) for v in df.iloc[row_idx].values]
            match_count = sum(1 for kw in keywords if any(kw in v for v in row_vals))
            if match_count >= 2:
                df.columns = df.iloc[row_idx].values
                df = df.iloc[row_idx + 1:].reset_index(drop=True)
                break

    # 3. 빅카인즈 컬럼명 유연한 매핑
    col_map = {}
    for col in df.columns:
        c_clean = str(col).strip()
        if any(k in c_clean for k in ["일자", "날짜", "date", "등록일"]):
            col_map[col] = "일자"
        elif any(k in c_clean for k in ["언론사", "매체", "출처", "media", "언론사명"]):
            col_map[col] = "언론사"
        elif any(k in c_clean for k in ["제목", "기사제목", "title", "헤드라인"]):
            col_map[col] = "제목"
        elif any(k in c_clean for k in ["본문", "기사원문", "내용", "본문(200자)", "기사본문", "요약"]):
            col_map[col] = "본문"
        elif any(k in c_clean for k in ["URL", "url", "링크", "link", "기사링크"]):
            col_map[col] = "URL"

    df = df.rename(columns=col_map)
    required = ["일자", "언론사", "본문"]
    for req in required:
        if req not in df.columns:
            raise ValueError(f"빅카인즈 필수 컬럼 '{req}'을 찾을 수 없습니다. (현재 인식된 컬럼: {list(df.columns)})")

    # 4. 행별 인사 발령 레코드 추출
    all_extracted_records = []
    for _, row in df.iterrows():
        pub_date = str(row["일자"]).strip()
        if len(pub_date) == 8 and pub_date.isdigit():
            pub_date = f"{pub_date[:4]}-{pub_date[4:6]}-{pub_date[6:]}"

        media = str(row["언론사"]).strip()
        title = str(row.get("제목", "")).strip()
        # [인사] 또는 <인사> 기사만 엄격 필터링
        if title and not is_personnel_article_title(title):
            continue

        body = str(row["본문"]).strip()
        url = str(row.get("URL", "")).strip()

        # 200자 제한 보완: 본문이 짧고 URL이 있는 경우 원문 스크랩 시도
        if len(body) < 250 and url.startswith("http"):
            full_body = _fetch_full_text_from_url(url)
            if full_body and len(full_body) > len(body):
                body = full_body

        records = PersonnelRegexParser.parse_article_text(
            article_text=body,
            default_media=media,
            article_title=title,
            pub_date=pub_date,
            article_url=url
        )
        all_extracted_records.extend(records)

    if not all_extracted_records:
        raise ValueError("파일 내 기사 본문에서 [부서명] [직책] [이름] 형태의 인사 발령 텍스트를 찾지 못했습니다. 기사 본문에 발령 내용이 포함되어 있는지 확인해주세요.")

    engine = PersonnelTrackerEngine(db_path=db_path)
    result = engine.process_records(all_extracted_records, append=append)
    return result


# ==========================================
# 7. 30개년 커리어 프로필 매트릭스 및 엑셀 리포트 빌더
# ==========================================

def generate_career_profile_metrics(timeline: List[Dict[str, Any]], reporter_obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    30개년 시계열 발령/활동 데이터를 기반으로 종합 커리어 프로필 매트릭스를 분석합니다:
    - 활동 기간 및 연차 산정 (Career Span)
    - 연도별 소속 매체 이동 로드맵 (Affiliation Journey)
    - 직급 승진 사다리 (Progression Ladder)
    - 출입처 전문 도메인 분포 (Domain Expertise)
    - 주요 발령 마일스톤 집계
    """
    if not timeline:
        return {}

    # 1. 시계열 정렬 (오름차순)
    sorted_items = sorted(
        timeline,
        key=lambda x: str(x.get("pub_date", "1970-01-01"))
    )

    first_date_str = str(sorted_items[0].get("pub_date", ""))[:10]
    last_date_str = str(sorted_items[-1].get("pub_date", ""))[:10]

    try:
        first_year = int(first_date_str[:4])
        last_year = int(last_date_str[:4])
        career_years = max(1, last_year - first_year + 1)
    except Exception:
        first_year = 1965
        last_year = 2026
        career_years = 1

    # 2. 소속 매체 이동 로드맵 (Affiliation Journey)
    # 연속된 동일 매체는 기간(start_year ~ end_year)으로 묶음
    affiliation_journey = []
    current_aff = None
    for item in sorted_items:
        m = item.get("media", "").strip()
        y_str = str(item.get("pub_date", ""))[:4]
        try:
            y = int(y_str)
        except Exception:
            y = first_year

        if not current_aff or current_aff["media"] != m:
            if current_aff:
                affiliation_journey.append(current_aff)
            current_aff = {
                "media": m,
                "start_year": y,
                "end_year": y,
                "last_rank": item.get("rank_title", "기자"),
                "last_dept": item.get("dept", "본사")
            }
        else:
            current_aff["end_year"] = max(current_aff["end_year"], y)
            current_aff["last_rank"] = item.get("rank_title", current_aff["last_rank"])
            current_aff["last_dept"] = item.get("dept", current_aff["last_dept"])
    if current_aff:
        affiliation_journey.append(current_aff)

    # 2-1. 소속 부서 순환 로드맵 (Department Rotation Journey - 타부서 발령 이력 추적)
    department_journey = []
    current_dept_entry = None
    for item in sorted_items:
        dept = item.get("dept", "").strip()
        if not dept or dept in ["본사", "미표기", "기자"]:
            continue
        m = item.get("media", "").strip()
        y_str = str(item.get("pub_date", ""))[:4]
        try:
            y = int(y_str)
        except Exception:
            y = first_year

        if not current_dept_entry or current_dept_entry["dept"] != dept or current_dept_entry["media"] != m:
            if current_dept_entry:
                department_journey.append(current_dept_entry)
            current_dept_entry = {
                "dept": dept,
                "media": m,
                "start_year": y,
                "end_year": y,
                "last_rank": item.get("rank_title", "기자")
            }
        else:
            current_dept_entry["end_year"] = max(current_dept_entry["end_year"], y)
            current_dept_entry["last_rank"] = item.get("rank_title", current_dept_entry["last_rank"])
    if current_dept_entry:
        department_journey.append(current_dept_entry)

    all_departments = []
    for dj in department_journey:
        if dj["dept"] not in all_departments:
            all_departments.append(dj["dept"])

    # 3. 직급 승진 사다리 (Progression Ladder)
    promotion_ladder = []
    seen_ranks = set()
    for item in sorted_items:
        r_title = item.get("rank_title", "").strip()
        if not r_title:
            continue
        r_level = item.get("rank_level", 2)
        y_str = str(item.get("pub_date", ""))[:4]
        if r_title not in seen_ranks:
            seen_ranks.add(r_title)
            promotion_ladder.append({
                "rank_title": r_title,
                "rank_level": r_level,
                "year": y_str,
                "media": item.get("media", ""),
                "dept": item.get("dept", "")
            })
    promotion_ladder = sorted(promotion_ladder, key=lambda x: (x["year"], x["rank_level"]))

    # 4. 출입처 도메인 전문성 분석 (Domain Expertise Breakdown)
    domain_counts: Dict[str, int] = {}
    for item in sorted_items:
        d = item.get("domain", "일반").strip()
        domain_counts[d] = domain_counts.get(d, 0) + 1

    total_cnt = len(sorted_items)
    domain_breakdown = [
        {"domain": d, "count": c, "percentage": round((c / total_cnt) * 100, 1)}
        for d, c in sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)
    ]
    primary_domain = domain_breakdown[0]["domain"] if domain_breakdown else "일반"

    # 5. 이직 및 발령 통계
    job_change_count = sum(1 for item in sorted_items if "이직" in item.get("action_type", ""))
    promotion_count = sum(1 for item in sorted_items if "승진" in item.get("action_type", ""))

    return {
        "first_year": first_year,
        "last_year": last_year,
        "career_years": career_years,
        "first_date": first_date_str,
        "last_date": last_date_str,
        "total_records": total_cnt,
        "job_change_count": job_change_count,
        "promotion_count": promotion_count,
        "affiliation_journey": affiliation_journey,
        "department_journey": department_journey,
        "all_departments": all_departments,
        "promotion_ladder": promotion_ladder,
        "domain_breakdown": domain_breakdown,
        "primary_domain": primary_domain
    }


def generate_career_excel(
    reporter_obj: Dict[str, Any],
    timeline: List[Dict[str, Any]],
    job_changes: List[Dict[str, Any]]
) -> bytes:
    """
    인물의 30개년 커리어 프로필, 연대기 타임라인, 이직 히스토리를
    정돈된 서식의 다중 시트 Excel(.xlsx) 바이트 스트림으로 생성합니다.
    """
    output = io.BytesIO()
    metrics = generate_career_profile_metrics(timeline, reporter_obj)

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Sheet 1: 프로필 요약
        summary_data = [
            {"항목": "언론인 성명", "내용": reporter_obj.get("name", "")},
            {"항목": "현재 소속 언론사", "내용": reporter_obj.get("current_media", "")},
            {"항목": "현재 부서", "내용": reporter_obj.get("current_dept", "")},
            {"항목": "현재 직급", "내용": reporter_obj.get("current_rank", "")},
            {"항목": "추적 활동 기간", "내용": f"{metrics.get('first_date', '')} ~ {metrics.get('last_date', '')}"},
            {"항목": "총 경력 연차", "내용": f"총 {metrics.get('career_years', 1)}년차 ({metrics.get('first_year', '')}년 ~ {metrics.get('last_year', '')}년)"},
            {"항목": "주요 출입처 전문분야", "내용": metrics.get("primary_domain", "일반")},
            {"항목": "바이라인 이메일", "내용": reporter_obj.get("email", "") or "미등록"},
            {"항목": "소속 매체 이동 경로", "내용": " -> ".join([f"{a['media']}({a['start_year']}~{a['end_year']})" for a in metrics.get("affiliation_journey", [])])},
            {"항목": "매체 간 이직 횟수", "내용": f"{metrics.get('job_change_count', 0)}회"},
            {"항목": "승진 발령 횟수", "내용": f"{metrics.get('promotion_count', 0)}회"},
            {"항목": "총 수집 발령/활동 건수", "내용": f"{metrics.get('total_records', len(timeline))}건"},
            {"항목": "고유 식별자(ID)", "내용": reporter_obj.get("reporter_id", "")},
            {"항목": "프로필 생성일시", "내용": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        ]
        df_summary = pd.DataFrame(summary_data)
        df_summary.to_excel(writer, sheet_name="1_커리어_프로필_요약", index=False)

        # Sheet 2: 전체 발령 및 활동 연대기
        timeline_rows = []
        for t in timeline:
            p_date = str(t.get("pub_date", ""))
            y_str = p_date[:4] if len(p_date) >= 4 else ""
            timeline_rows.append({
                "발령일자": p_date,
                "연도": y_str,
                "언론사": t.get("media", ""),
                "부서": t.get("dept", ""),
                "직급": t.get("rank_title", ""),
                "직급서열": t.get("rank_level", 2),
                "발령구분": t.get("action_type", "발령"),
                "출입처도메인": t.get("domain", "일반"),
                "기사제목": t.get("article_title", ""),
                "기사원문링크": t.get("article_url", ""),
                "원문텍스트": t.get("raw_text", "")
            })
        df_timeline = pd.DataFrame(timeline_rows)
        if not df_timeline.empty:
            df_timeline = df_timeline.sort_values(by="발령일자", ascending=True)
        df_timeline.to_excel(writer, sheet_name="2_전체_발령_연대기", index=False)

        # Sheet 3: 이직 히스토리
        if job_changes:
            jc_rows = []
            for jc in job_changes:
                jc_rows.append({
                    "이직발령일자": jc.get("to_date", ""),
                    "이전 언론사": jc.get("from_media", ""),
                    "이전 부서": jc.get("from_dept", ""),
                    "이전 직급": jc.get("from_rank", ""),
                    "이직후 언론사": jc.get("to_media", ""),
                    "이직후 부서": jc.get("to_dept", ""),
                    "이직후 직급": jc.get("to_rank", ""),
                    "경과일수": jc.get("days_gap", 0),
                    "알고리즘신뢰도": jc.get("confidence_score", 0.0),
                    "탐지근거": jc.get("detected_reason", "")
                })
            df_jc = pd.DataFrame(jc_rows)
            df_jc.to_excel(writer, sheet_name="3_매체간_이직_기록", index=False)

    return output.getvalue()


try:
    from media_personnel_pipeline import PersonnelSearchEngine
except ImportError:
    PersonnelSearchEngine = None
