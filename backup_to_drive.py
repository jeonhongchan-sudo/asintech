# backup_to_drive.py
import os
import requests
import json
import base64
from postgrest import SyncPostgrestClient

# 1. 환경 변수 로드 (GitHub Secrets에서 가져올 값들)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
GAS_URL = os.environ.get("GAS_URL")
R2_PUBLIC_URL = os.environ.get("R2_BASE_URL", "https://pub-8664539130754807963d76c944415843.r2.dev")

def main():
    # Supabase 클라이언트 초기화
    client = SyncPostgrestClient(f"{SUPABASE_URL}/rest/v1", headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    })

    print("🔍 백업 대기 중인 메모 조회 중...")
    
    # 2. 백업 대상 조회 (is_survey=true AND backup_status='pending')
    try:
        response = client.table("memos")\
            .select("id, project_id, image_url, content")\
            .eq("is_survey", True)\
            .eq("backup_status", "pending")\
            .execute()
        
        memos = response.data
    except Exception as e:
        print(f"❌ DB 조회 오류: {e}")
        return

    if not memos:
        print("✅ 백업할 항목이 없습니다.")
        return

    print(f"📦 총 {len(memos)}건의 메모를 처리합니다.")

    for memo in memos:
        memo_id = memo['id']
        project_id = memo['project_id']
        image_urls = memo.get('image_url', '').split(',')
        
        print(f"\n--- 메모 ID: {memo_id} (프로젝트: {project_id}) 처리 시작 ---")
        
        # 상태를 'processing'으로 변경 (중복 실행 방지)
        client.table("memos").update({"backup_status": "processing"}).eq("id", memo_id).execute()

        success_count = 0
        drive_ids = []

        for url in image_urls:
            url = url.strip()
            if not url or "r2.dev" not in url:
                continue

            # 3. R2 원본 파일 경로 추출 및 다운로드
            # URL 구조: .../preview/uuid/filename.webp -> .../orig/uuid/filename.jpg
            try:
                orig_url = url.replace("/preview/", "/orig/").replace(".webp", ".jpg")
                file_name = orig_url.split('/')[-1]
                
                print(f"📥 R2에서 원본 다운로드 중: {file_name}")
                file_res = requests.get(orig_url)
                if file_res.status_code != 200:
                    raise Exception(f"R2 다운로드 실패 ({file_res.status_code})")
                
                # 4. GAS(구글 드라이브)로 전송
                print(f"📤 구글 드라이브로 백업 전송 중...")
                file_data_base64 = base64.b64encode(file_res.content).decode('utf-8')
                
                gas_payload = {
                    "action": "uploadToDrive",
                    "fileName": file_name,
                    "fileData": file_data_base64,
                    "mimeType": "image/jpeg",
                    "projectId": str(project_id)
                }
                
                # GAS는 POST 요청 시 쿼리 파라미터로 action을 받는 경우가 많으므로 URL에 포함
                gas_res = requests.post(f"{GAS_URL}?action=uploadToDrive", json=gas_payload)
                gas_result = gas_res.json()

                if gas_result.get("success"):
                    print(f"✅ 백업 완료: {gas_result.get('fileId')}")
                    drive_ids.append(gas_result.get('fileId'))
                    success_count += 1
                else:
                    raise Exception(f"GAS 오류: {gas_result.get('error')}")

            except Exception as e:
                error_msg = f"사진 백업 실패: {str(e)}"
                print(f"❌ {error_msg}")
                client.table("memos").update({
                    "backup_status": "failed",
                    "backup_error": error_msg
                }).eq("id", memo_id).execute()
                continue

        # 5. 모든 사진 백업 성공 시 최종 상태 업데이트
        if success_count > 0:
            client.table("memos").update({
                "backup_status": "completed",
                "backup_drive_id": ",".join(drive_ids),
                "backup_error": None
            }).eq("id", memo_id).execute()
            print(f"🎊 메모 {memo_id} 백업 전체 완료!")

if __name__ == "__main__":
    main()
