import os
import sys
import json
import subprocess
import boto3
import requests
from botocore.client import Config
import shapely.wkt
import shapely.geometry

# 환경 변수 로드 (GitHub Secrets에서 주입됨)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME")

# [추가] 필수 환경 변수 검증 로직
required_vars = {
    "SUPABASE_URL": SUPABASE_URL,
    "SUPABASE_KEY": SUPABASE_KEY,
    "R2_ACCOUNT_ID": R2_ACCOUNT_ID,
    "R2_ACCESS_KEY_ID": R2_ACCESS_KEY_ID,
    "R2_SECRET_ACCESS_KEY": R2_SECRET_ACCESS_KEY,
    "R2_BUCKET_NAME": R2_BUCKET_NAME
}

missing = [key for key, val in required_vars.items() if not val]
if missing:
    print(f"Error: 다음 환경 변수들이 GitHub Secrets에 설정되지 않았습니다: {', '.join(missing)}")
    sys.exit(1)

def download_json(project_id):
    """Supabase Storage에서 JSON 파일 다운로드"""
    print(f"Downloading JSON for Project {project_id}...")
    
    # Supabase Storage URL 구성
    file_name = f"CAD_{project_id}.json"
    url = f"{SUPABASE_URL}/storage/v1/object/cad_layers/{file_name}"
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        with open("input.json", "wb") as f:
            f.write(response.content)
        print("Download complete.")
        return True
    else:
        print(f"Error downloading file: {response.status_code} - {response.text}")
        return False

def convert_to_geojson():
    """다운로드한 JSON(리스트)을 GeoJSON(FeatureCollection)으로 변환"""
    print("Converting raw JSON to GeoJSON...")
    try:
        with open("input.json", "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        
        # 이미 GeoJSON 형식이면 그대로 저장
        if isinstance(raw_data, dict) and raw_data.get('type') == 'FeatureCollection':
            print("Input is already GeoJSON.")
            with open("input.geojson", "w", encoding="utf-8") as f:
                json.dump(raw_data, f)
            return True

        features = []
        if isinstance(raw_data, list):
            for item in raw_data:
                geom = None
                # 1. WKT 파싱 시도
                wkt_raw = item.get('wkt') or item.get('geom')
                if wkt_raw:
                    # SRID 제거 (SRID=4326;POINT(...) -> POINT(...))
                    if wkt_raw.startswith('SRID='):
                        wkt_raw = wkt_raw.split(';', 1)[1]
                    try:
                        g = shapely.wkt.loads(wkt_raw)
                        geom = shapely.geometry.mapping(g)
                    except Exception as e:
                        print(f"Failed to parse WKT: {e}")
                
                # 2. WKT가 없으면 x, y 좌표 사용
                if not geom and 'x' in item and 'y' in item:
                    try:
                        geom = {"type": "Point", "coordinates": [float(item['x']), float(item['y'])]}
                    except:
                        pass
                
                if geom:
                    features.append({ "type": "Feature", "geometry": geom, "properties": item })
        
        geojson = { "type": "FeatureCollection", "features": features }
        
        with open("input.geojson", "w", encoding="utf-8") as f:
            json.dump(geojson, f)
        print(f"Converted {len(features)} items to GeoJSON.")
        return True
    except Exception as e:
        print(f"GeoJSON conversion error: {e}")
        return False

def convert_to_pmtiles():
    """Tippecanoe를 사용하여 JSON을 PMTiles로 변환"""
    print("Converting to PMTiles...")
    
    # Tippecanoe 명령어 실행
    # -zg: 줌 레벨 자동 추측
    # --drop-rate=1: 객체 생략 없이 모든 데이터 유지 (CAD 도면 특성상 중요)
    # -l cad_data: 레이어 이름 지정
    cmd = [
        "tippecanoe",
        "-o", "output.pmtiles",
        "-l", "cad_data",
        "--force",
        "-zg", 
        "--drop-rate=1",
        "input.geojson"
    ]
    
    try:
        subprocess.run(cmd, check=True)
        print("Conversion complete.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Conversion failed: {e}")
        return False

def upload_to_r2(project_id):
    """Cloudflare R2에 PMTiles 업로드"""
    print("Uploading to R2...")
    
    s3 = boto3.client(
        's3',
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4')
    )
    
    file_name = f"CAD_{project_id}.pmtiles"
    
    try:
        with open("output.pmtiles", "rb") as f:
            s3.upload_fileobj(
                f, 
                R2_BUCKET_NAME, 
                file_name,
                ExtraArgs={'ContentType': 'application/vnd.pmtiles'}
            )
        print(f"Upload success: {file_name}")
        return True
    except Exception as e:
        print(f"Upload failed: {e}")
        return False

if __name__ == "__main__":
    # 커맨드라인 인자로 프로젝트 ID 받기
    if len(sys.argv) < 2:
        print("Usage: python convert_r2.py <project_id>")
        sys.exit(1)
        
    project_id = sys.argv[1]
    
    if download_json(project_id):
        if convert_to_geojson(): # [추가] GeoJSON 변환 단계
            if convert_to_pmtiles():
                if upload_to_r2(project_id):
                    print("All steps completed successfully.")
                else:
                    sys.exit(1)
            else:
                sys.exit(1)
        else:
            sys.exit(1)
