import os
import sys
import json
import ezdxf
import boto3
import subprocess
from supabase import create_client

# 환경변수 로드
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
ODA_PATH = os.environ.get("ODA_PATH", "/usr/local/bin/ODAFileConverter")

def get_r2_client():
    return boto3.client('s3', endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
                        aws_access_key_id=R2_ACCESS_KEY_ID, aws_secret_access_key=R2_SECRET_ACCESS_KEY)

def analyze(payload):
    project_id = payload.get('project_id')
    file_path = payload.get('file_path')
    file_name = payload.get('file_name', '').lower()
    
    r2 = get_r2_client()
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 작업 디렉토리 생성
    os.makedirs("input_dir", exist_ok=True)
    os.makedirs("output_dir", exist_ok=True)
    
    local_raw = os.path.join("input_dir", file_name)
    dxf_file = ""

    try:
        # 1. R2에서 원본 파일 다운로드
        print(f"📥 Downloading {file_name} from R2...")
        r2.download_file(R2_BUCKET_NAME, file_path, local_raw)
        
        # 2. DWG인 경우 DXF로 변환
        if file_name.endswith('.dwg'):
            print("🔄 Converting DWG to DXF using ODA File Converter...")
            # ODA 전용 명령 (가상 디스플레이 xvfb-run 사용)
            # 인자: input_dir, output_dir, version, type, recurse, audit
            cmd = f"xvfb-run {ODA_PATH} ./input_dir ./output_dir \"ACAD2018\" \"DXF\" \"0\" \"1\""
            subprocess.run(cmd, shell=True, check=True)
            
            # 변환된 파일명 찾기 (입력파일명.dxf 로 생성됨)
            dxf_name = file_name.replace('.dwg', '.dxf')
            dxf_file = os.path.join("output_dir", dxf_name)
        else:
            dxf_file = local_raw

        if not os.path.exists(dxf_file):
            raise Exception("DXF file not found after conversion/download.")

        # 3. 레이어 추출 (ezdxf)
        print("🔍 Extracting layers...")
        doc = ezdxf.readfile(dxf_file)
        # 빈 레이어 제외 및 정렬
        layers = [layer.dxf.name for layer in doc.layers if layer.dxf.name != '0']
        layers.sort()
        
        # 4. 분석용 DXF를 R2의 표준 경로로 업로드 (나중에 convert_r2.py가 쓸 수 있게)
        # 기존 convert_r2.py는 cad_data/CAD_{project_id}.dxf 경로를 사용함
        target_dxf_key = f"cad_data/CAD_{project_id}.dxf"
        print(f"📤 Uploading processed DXF to {target_dxf_key}...")
        r2.upload_file(dxf_file, R2_BUCKET_NAME, target_dxf_key)

        # [추가] DWG 파일인 경우 분석 완료 후 R2에서 원본 자동 삭제
        if file_name.endswith('.dwg'):
            try:
                print(f"🗑️ Deleting original DWG from R2: {file_path}")
                r2.delete_object(Bucket=R2_BUCKET_NAME, Key=file_path)
            except Exception as delete_err:
                print(f"⚠️ Failed to delete DWG: {delete_err}")

        # 5. Supabase 업데이트 (레이어 목록 저장 및 상태 변경)
        print(f"✅ Updating Supabase status for project {project_id}...")
        supabase.table("cad_projects").update({
            "status": "ANALYZED",
            "available_layers": layers
        }).eq("id", project_id).execute()
        
        print(f"🎉 Analysis Complete. Found {len(layers)} layers.")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        try:
            supabase.table("cad_projects").update({"status": "ERROR"}).eq("id", project_id).execute()
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