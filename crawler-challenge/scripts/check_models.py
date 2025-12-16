# check_models.py
import os
import google.generativeai as genai
from dotenv import load_dotenv

# .env 파일이 있다면 로드
load_dotenv()

# API 키 설정 (환경변수에 없으면 직접 문자열로 넣어서 테스트해보세요)
api_key = os.getenv("GEMINI_API_KEY") 
# api_key = "AIzaSy..." # 직접 입력해서 테스트할 경우 주석 해제

if not api_key:
    print("❌ API Key가 없습니다. 설정해주세요.")
else:
    genai.configure(api_key=api_key)
    print(f"🔑 API Key 확인됨: {api_key[:10]}...")

    print("\n🔍 사용 가능한 모델 목록:")
    try:
        # 사용 가능한 모델을 나열하고 generateContent 기능이 있는 것만 필터링
        for m in genai.list_models():
            if 'generateContent' in m.supported_generation_methods:
                print(f" - {m.name}")
    except Exception as e:
        print(f"❌ 목록 조회 실패: {e}")