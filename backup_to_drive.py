import os
import requests
import base64
from postgrest import SyncPostgrestClient

# 환경 변수
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
GAS_URL = os.environ.get("GAS_URL")

def main():
    client = SyncPostgrestClient(f"{SUPABASE_URL}/rest/v1", headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    })

    print("🔍 백업 대기 중인 메모 조회...")
    memos = client.table("memos").select("id, project_id, image_url").eq("backup_status", "pending").execute().data

    if not memos:
        print("✅ 백업할 항목이 없습니다.")
        return

    for memo in memos:
        memo_id = memo['id']
        project_id = str(memo['project_id'])
        urls = memo.get('image_url', '').split(',')
        
        client.table("memos").update({"backup_status": "processing"}).eq("id", memo_id).execute()

        for url in urls:
            url = url.strip()
            if "r2.dev" not in url: continue
            
            orig_url = url.replace("/preview/", "/orig/").replace(".webp", ".jpg")
            file_name = orig_url.split('/')[-1]

            # [중복 체크] 이미 성공한 백업이 있는지 확인
            existing = client.table("backup_logs").select("id").eq("r2_url", url).eq("status", "completed").execute().data
            if existing:
                print(f"⏩ 스킵 (이미 백업됨): {file_name}")
                continue

            try:
                # 1. R2 다운로드
                res = requests.get(orig_url)
                if res.status_code != 200: raise Exception("R2 다운로드 실패")

                # 2. GAS 업로드
                gas_payload = {
                    "action": "uploadToDrive",
                    "fileName": file_name,
                    "fileData": base64.b64encode(res.content).decode('utf-8'),
                    "mimeType": "image/jpeg",
                    "projectId": project_id
                }
                gas_res = requests.post(f"{GAS_URL}?action=uploadToDrive", json=gas_payload).json()

                if gas_res.get("success"):
                    # 3. backup_logs 테이블 기록
                    log_data = {
                        "memo_id": memo_id,
                        "project_id": project_id,
                        "file_name": file_name,
                        "r2_url": url,
                        "drive_file_id": gas_res.get("fileId"),
                        "status": "completed"
                    }
                    client.table("backup_logs").upsert(log_data, on_conflict="r2_url").execute()
                    print(f"✅ 백업 완료: {file_name}")
                else:
                    raise Exception(gas_res.get("error"))

            except Exception as e:
                print(f"❌ 실패: {file_name} ({e})")
                client.table("backup_logs").upsert({
                    "memo_id": memo_id, "project_id": project_id, "file_name": file_name,
                    "r2_url": url, "status": "failed", "error_message": str(e)
                }, on_conflict="r2_url").execute()

        # 메모 상태 업데이트
        client.table("memos").update({"backup_status": "completed"}).eq("id", memo_id).execute()

if __name__ == "__main__":
    main()
