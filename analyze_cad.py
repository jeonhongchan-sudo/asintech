import os
import sys
import json
import ezdxf
import boto3
from datetime import datetime, timezone
from supabase import create_client

# 환경변수 로드
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_r2_client():
    return boto3.client('s3', endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
                        aws_access_key_id=R2_ACCESS_KEY_ID, aws_secret_access_key=R2_SECRET_ACCESS_KEY)

def analyze(payload):
    project_id = payload.get('project_id')
    file_path = payload.get('file_path')
    file_name = payload.get('file_name', '').lower()
    
    r2 = get_r2_client()
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 작업 디렉토리 초기화
    os.makedirs("input_dir", exist_ok=True)
    
    local_raw = os.path.join("input_dir", file_name)
    # [수정] ODA 변환 로직을 제거하고 업로드된 파일을 직접 DXF로 처리
    dxf_file = local_raw

    try:
        # 1. R2에서 DXF 파일 다운로드
        print(f"📥 Downloading DXF file '{file_name}' from R2...")
        r2.download_file(R2_BUCKET_NAME, file_path, local_raw)

        # 2. 레이어 추출 (ezdxf) 및 인코딩 복구
        print("🔍 Extracting layers...")
        doc = ezdxf.readfile(dxf_file)
        
        layers = []
        for layer in doc.layers:
            name = layer.dxf.name
            if name == '0': continue
            
            # [추가] \M+... (MIF) 인코딩이 발견되면 한글로 디코딩
            if "\\M+" in name:
                try:
                    name = ezdxf.tools.encoding.decode_mif(name)
                except: pass
            
            layers.append(name)
            
        layers.sort()

        # [추가] DXF 최적화 재저장
        print("💾 Sanitizing DXF encoding and structure...")
        doc.saveas(dxf_file)
        
        # 3. 분석 완료된 DXF를 표준 경로로 업로드
        target_dxf_key = f"cad_data/CAD_{project_id}.dxf"
        print(f"📤 Uploading processed DXF to {target_dxf_key}...")
        # [수정] Content-Type을 application/dxf로 명시하여 업로드
        r2.upload_file(dxf_file, R2_BUCKET_NAME, target_dxf_key, ExtraArgs={'ContentType': 'application/dxf'})

        # 4. Supabase cad_files 테이블 정보 갱신
        try:
            file_size = os.path.getsize(dxf_file)
            file_metadata = {
                "project_id": int(project_id),
                "file_type": "dxf",
                "file_path": target_dxf_key,
                "file_size": file_size,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            res = supabase.table("cad_files").select("id").eq("file_path", target_dxf_key).execute()
            if res.data: supabase.table("cad_files").update(file_metadata).eq("file_path", target_dxf_key).execute()
            else: supabase.table("cad_files").insert(file_metadata).execute()
            print(f"✅ DXF metadata registered in cad_files table.")
        except Exception as db_err:
            print(f"⚠️ Failed to update cad_files metadata: {db_err}")

        # [추가] 원본 업로드 파일이 임시 경로인 경우 삭제 (Cleanup)
        if file_path != target_dxf_key:
             try:
                 print(f"🗑️ Deleting temporary upload file from R2: {file_path}")
                 r2.delete_object(Bucket=R2_BUCKET_NAME, Key=file_path)
             except Exception as delete_err:
                 print(f"⚠️ Failed to delete temp file: {delete_err}")

        # 5. 최종 프로젝트 상태 업데이트
        print(f"✅ Updating Supabase status for project {project_id}...")
        supabase.table("cad_projects").update({
            "status": "ANALYZED",
            "available_layers": layers
        }).eq("id", project_id).execute()
        
        print(f"🎉 Analysis Complete. Found {len(layers)} layers.")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        try:
            # [추가] Supabase 업데이트 실패 시에도 로그를 남김
            error_message = str(e)
            print(f"Attempting to update Supabase project status to ERROR: {error_message}")
            supabase.table("cad_projects").update({"status": "ERROR", "error_details": error_message}).eq("id", project_id).execute()
            print(f"Successfully updated project {project_id} status to ERROR in Supabase.")
        except: pass
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyze_cad.py '<json_payload>'")
        sys.exit(1)
    
    try:
        payload = json.loads(sys.argv[1])
        analyze(payload)
    except Exception as e:
        print(f"Payload error: {e}")
        sys.exit(1)