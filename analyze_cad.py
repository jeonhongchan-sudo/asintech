import os
import sys
import json
import ezdxf
import boto3
import shutil
import subprocess
from datetime import datetime, timezone
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

    # ODA_PATH 확인 및 자동 탐색 로직 개선
    actual_oda_path = payload.get('oda_path') or ODA_PATH
    # .desktop 파일이 걸려오는 경우를 대비해 유효성 검사 추가
    if not actual_oda_path or not os.path.exists(actual_oda_path) or actual_oda_path.endswith('.desktop'):
        # PATH에서 검색
        found_in_path = shutil.which("ODAFileConverter")
        if found_in_path:
            actual_oda_path = found_in_path
        else:
            # 일반적인 설치 경로 순차 확인
            for p in ["/usr/local/bin/ODAFileConverter", "/usr/bin/ODAFileConverter"]:
                if os.path.exists(p):
                    actual_oda_path = p
                    break

    try:
        # 1. R2에서 원본 파일 다운로드
        print(f"📥 Downloading {file_name} from R2...")
        r2.download_file(R2_BUCKET_NAME, file_path, local_raw)
        
        # 2. DWG인 경우 DXF로 변환
        if file_name.endswith('.dwg'):
            if not actual_oda_path or not os.path.exists(actual_oda_path):
                raise Exception(f"ODA File Converter not found. Path: {actual_oda_path}")

            print(f"🔄 Converting DWG to DXF using ODA at {actual_oda_path}...")
            # ODA 전용 명령 (가상 디스플레이 xvfb-run --auto-servernum 사용)
            # 인자: input_dir, output_dir, version, type, recurse, audit
            # [수정] 한글 인코딩 문제 및 레이어명 깨짐 해결을 위해 유니코드를 지원하는 ACAD2018 버전으로 출력
            cmd = f"xvfb-run --auto-servernum {actual_oda_path} ./input_dir ./output_dir \"ACAD2018\" \"DXF\" \"0\" \"1\""
            print(f"🚀 Running command: {cmd}")
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

        # [추가] DXF 파일 재저장 (Sanitize)
        # ezdxf로 다시 저장하면 ODA에서 발생할 수 있는 구조적 오류와 인코딩 헤더 문제를 해결하여 타 프로그램에서의 충돌을 방지합니다.
        print("💾 Sanitizing and re-saving DXF to fix encoding/corruption...")
        doc.saveas(dxf_file)
        
        # 4. 분석용 DXF를 R2의 표준 경로로 업로드 (나중에 convert_r2.py가 쓸 수 있게)
        # 기존 convert_r2.py는 cad_data/CAD_{project_id}.dxf 경로를 사용함
        target_dxf_key = f"cad_data/CAD_{project_id}.dxf"
        print(f"📤 Uploading processed DXF to {target_dxf_key}...")
        # [수정] Content-Type을 application/dxf로 명시하여 업로드
        r2.upload_file(dxf_file, R2_BUCKET_NAME, target_dxf_key, ExtraArgs={'ContentType': 'application/dxf'})

        # [추가] Supabase cad_files 테이블에 DXF 정보 등록 (convert_r2.py 연동용)
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