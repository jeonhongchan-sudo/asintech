import os
import sys
import io
import time
import requests
from sentence_transformers import SentenceTransformer
from supabase import create_client

# 1. 한글 인코딩 에러 ('ascii' codec) 방지를 위한 스트림 설정
# 특히 한글이 포함된 로그를 출력할 때 발생할 수 있는 오류를 차단합니다.
sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding='utf-8')

# GitHub Secrets 환경 변수
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# 로컬 임베딩 모델 로드 (다국어 지원: 한국어, 영어 등)
# 최초 실행 시 모델을 다운로드하며, 이후에는 로컬 캐시를 사용합니다.
print("[*] 로컬 임베딩 엔진(Multilingual MPNet) 로드 중...")
model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')

def get_local_embedding(text):
    """로컬 엔진을 사용하여 768차원 임베딩 생성 (API 키 불필요)"""
    # 한글/영문 텍스트를 벡터로 변환
    embedding = model.encode(text)
    return embedding.tolist()

def run_auto_embedding():
    if not all([SUPABASE_URL, SUPABASE_KEY]):
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

        print(f"[+] 총 {len(targets)}건의 누락된 벡터를 생성합니다 (로컬 엔진 - 무제한)...")

        for item in targets:
            try:
                # 2. 로컬 엔진으로 벡터 생성
                vector = get_local_embedding(item['content'])
                
                # 3. DB 업데이트
                supabase_client.table("pdf_knowledge").update({"embedding": vector}).eq("id", item['id']).execute()
                print(f"    - ID {item['id']} 처리 완료")
                
                # 로컬 작업이므로 지연 시간(sleep)이 필요 없으나, DB 부하를 위해 0.1초만 대기
                time.sleep(0.1)
            except Exception as e:
                print(f"    - [!] ID {item['id']} 처리 중 오류: {e}")

        print("\n[✅ 완료] 임베딩 동기화 작업이 성공적으로 끝났습니다.")
    except Exception as e:
        print(f"[❌ 치명적 오류] {e}")

if __name__ == "__main__":
    run_auto_embedding()
