import os
import sys
import io
import time
import requests
from supabase import create_client

# 1. 한글 인코딩 에러 ('ascii' codec) 방지를 위한 스트림 설정
# 특히 한글이 포함된 로그를 출력할 때 발생할 수 있는 오류를 차단합니다.
sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding='utf-8')

# GitHub Secrets 환경 변수
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") # [추가] 구글 API 키 필요

def get_gemini_embedding(text, max_retries=3):
    """구글 Gemini API를 사용하여 768차원 임베딩 생성 (429 에러 시 재시도 로직 포함)"""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?key={GEMINI_API_KEY}"
    payload = {
        "model": "models/gemini-embedding-001",
        "content": {"parts": [{"text": text}]}
    }
    
    for attempt in range(max_retries):
        res = requests.post(url, json=payload)
        if res.status_code == 200:
            return res.json()['embedding']['values']
        elif res.status_code == 429:
            # 무료 티어는 분당 요청 제한이 엄격하므로, 60초 이상 대기 후 재시도합니다.
            wait_time = 60 * (attempt + 1)
            print(f"    - [!] Rate Limit (429) 발생. {wait_time}초 대기 후 재시도 ({attempt + 1}/{max_retries})...")
            time.sleep(wait_time)
        else:
            raise Exception(f"Gemini API Error: {res.text}")
            
    raise Exception(f"최대 재시도 횟수({max_retries})를 초과했습니다. API 할당량을 확인하세요.")

def run_auto_embedding():
    if not all([SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY]):
        print("[❌ 오류] 환경 변수 설정 미비")
        return

    try:
        supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)

        # 1. 임베딩이 비어있는(null) 데이터 조회
        print("[*] 임베딩 작업이 필요한 데이터 조회 중...")
        res = supabase_client.table("pdf_knowledge").select("id, content").is_("embedding", "null").execute()
        targets = res.data

        if not targets:
            print("[✔] 모든 데이터가 업데이트되어 있습니다.")
            return

        print(f"[+] 총 {len(targets)}건의 누락된 벡터를 생성합니다 (Google Gemini 엔진)...")

        for item in targets:
            try:
                # 2. 구글 엔진으로 벡터 생성
                vector = get_gemini_embedding(item['content'])
                
                # 3. DB 업데이트
                supabase_client.table("pdf_knowledge").update({"embedding": vector}).eq("id", item['id']).execute()
                print(f"    - ID {item['id']} 처리 완료")
                
                # API 안정성을 위해 요청 사이에 짧은 지연 시간(2초) 추가
                # (Gemini 무료 티어: 분당 약 15회 요청 제한 대응)
                time.sleep(2)
            except Exception as e:
                print(f"    - [!] ID {item['id']} 처리 중 오류: {e}")

        print("\n[✅ 완료] 새벽 동기화 작업이 성공적으로 끝났습니다.")
    except Exception as e:
        print(f"[❌ 치명적 오류] {e}")

if __name__ == "__main__":
    run_auto_embedding()
