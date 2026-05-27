import os
import sys
import requests
from supabase import create_client

# GitHub Secrets 환경 변수
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") # [추가] 구글 API 키 필요

def get_gemini_embedding(text):
    """구글 Gemini API를 사용하여 768차원 임베딩 생성"""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?key={GEMINI_API_KEY}"
    payload = {
        "model": "models/gemini-embedding-001",
        "content": {"parts": [{"text": text}]}
    }
    res = requests.post(url, json=payload)
    if res.status_code == 200:
        return res.json()['embedding']['values']
    else:
        raise Exception(f"Gemini API Error: {res.text}")

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
            except Exception as e:
                print(f"    - [!] ID {item['id']} 처리 중 오류: {e}")

        print("\n[✅ 완료] 새벽 동기화 작업이 성공적으로 끝났습니다.")
    except Exception as e:
        print(f"[❌ 치명적 오류] {e}")

if __name__ == "__main__":
    run_auto_embedding()
