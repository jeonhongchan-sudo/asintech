import os
import requests
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from postgrest import SyncPostgrestClient

# 1. 환경 변수 로드
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
GAS_URL = os.environ.get("GAS_URL")

def process_single_image(client, memo_id, project_id, url):
    """개별 이미지를 다운로드하여 구글 드라이브로 백업하는 작업 단위 (Thread용)"""
    url = url.strip()
    if not url or "r2.dev" not in url:
        return True # 처리할 대상 아님

    # R2 경로 변환 (preview -> orig, webp -> jpg)
    orig_url = url.replace("/preview/", "/orig/").replace(".webp", ".jpg")
    file_name = orig_url.split('/')[-1]

    # [중복 체크] 이미 성공한 백업이 있는지 확인
    try:
        existing = client.table("backup_logs").select("id").eq("r2_url", url).eq("status", "completed").execute().data
        if existing:
            print(f"⏩ 스킵 (이미 백업됨): {file_name}")
            return True
    except Exception as e:
        print(f"⚠️ 중복 체크 실패: {file_name} ({e})")

    try:
        # 1. R2에서 원본 다운로드
        print(f"📥 다운로드 중: {file_name}")
        file_res = requests.get(orig_url, timeout=30)
        if file_res.status_code != 200:
            raise Exception(f"R2 다운로드 실패 ({file_res.status_code})")
        
        # 2. GAS(구글 드라이브)로 전송
        print(f"📤 드라이브 전송 중: {file_name}")
        file_data_base64 = base64.b64encode(file_res.content).decode('utf-8')
        
        gas_payload = {
            "action": "uploadToDrive",
            "fileName": file_name,
            "fileData": file_data_base64,
            "mimeType": "image/jpeg",
            "projectId": str(project_id)
        }
        
        # GAS 요청 (timeout 넉넉히 60초)
        gas_res = requests.post(f"{GAS_URL}?action=uploadToDrive", json=gas_payload, timeout=60).json()

        if gas_res.get("success"):
            # 3. backup_logs 테이블 기록 (UPSERT)
            log_data = {
                "memo_id": memo_id,
                "project_id": str(project_id),
                "file_name": file_name,
                "r2_url": url,
                "drive_file_id": gas_res.get("fileId"),
                "status": "completed",
                "updated_at": "now()"
            }
            client.table("backup_logs").upsert(log_data, on_conflict="r2_url").execute()
            print(f"✅ 백업 완료: {file_name}")
            return True
        else:
            raise Exception(gas_res.get("error"))

    except Exception as e:
        error_msg = str(e)
        print(f"❌ 실패: {file_name} ({error_msg})")
        # 실패 로그 기록
        try:
            client.table("backup_logs").upsert({
                "memo_id": memo_id, "project_id": str(project_id), "file_name": file_name,
                "r2_url": url, "status": "failed", "error_message": error_msg, "updated_at": "now()"
            }, on_conflict="r2_url").execute()
        except: pass
        return False

def main():
    # Supabase 클라이언트 초기화
    client = SyncPostgrestClient(f"{SUPABASE_URL}/rest/v1", headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    })

    print("🔍 백업 대기 중인 메모 조회...")
    memos = client.table("memos").select("id, project_id, image_url").eq("backup_status", "pending").execute().data

    if not memos:
        print("✅ 백업할 항목이 없습니다.")
        return

    print(f"📦 총 {len(memos)}건의 메모를 처리합니다. (4개 병렬 업로드 활성)")

    for memo in memos:
        memo_id = memo['id']
        project_id = memo['project_id']
        urls = [u.strip() for u in memo.get('image_url', '').split(',') if u.strip()]
        
        client.table("memos").update({"backup_status": "processing"}).eq("id", memo_id).execute()

        # [핵심] ThreadPoolExecutor를 사용하여 최대 4개까지 병렬 처리
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(process_single_image, client, memo_id, project_id, url) for url in urls]
            results = [f.result() for f in as_completed(futures)]

        # 모든 파일 처리 후 메모 상태 업데이트
        client.table("memos").update({"backup_status": "completed"}).eq("id", memo_id).execute()
        print(f"🎊 메모 {memo_id} 모든 파일 처리 완료!")

if __name__ == "__main__":
    main()