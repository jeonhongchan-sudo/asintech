import os
import sys
from supabase import create_client
from sentence_transformers import SentenceTransformer

# GitHub Secrets에서 정보를 가져옵니다.
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def run_auto_embedding():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[❌ 오류] 환경 변수가 설정되지 않았습니다. GitHub Secrets를 확인하세요.")
        return

    try:
        # 1. 로컬 임베딩 모델 로드 (할당량 걱정 없는 오픈소스 모델)
        print("[*] 임베딩 모델 로드 중 (paraphrase-multilingual-mpnet-base-v2)...")
        model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2')
        
        # 2. Supabase 클라이언트 초기화
        supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)

        # 3. 임베딩이 없는(null) 데이터 조회
        print("[*] 최적화가 필요한 데이터 조회 중...")
        res = supabase_client.table("pdf_knowledge").select("id, content").is_("embedding", "null").execute()
        targets = res.data

        if not targets:
            print("[✔] 모든 데이터가 이미 최적화되어 있습니다.")
            return

        print(f"[+] 총 {len(targets)}개의 누락된 지식을 발견했습니다. 벡터화 시작...")

        # 4. 루프를 돌며 빈칸 채우기
        for item in targets:
            try:
                # 로컬 연산으로 벡터 생성 (768차원)
                vector = model.encode(item['content']).tolist()
                
                # 해당 행 업데이트
                supabase_client.table("pdf_knowledge").update({"embedding": vector}).eq("id", item['id']).execute()
                print(f"    - ID {item['id']} 최적화 완료")
            except Exception as e:
                print(f"    - [!] ID {item['id']} 처리 중 오류: {e}")

        print("\n[✅ 완료] 자동 임베딩 작업이 성공적으로 끝났습니다.")
    except Exception as e:
        print(f"[❌ 치명적 오류] 작업 중 중단됨: {e}")

if __name__ == "__main__":
    run_auto_embedding()