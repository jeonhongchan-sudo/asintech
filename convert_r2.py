import os
import sys
import json
import subprocess
import boto3
import requests
from botocore.client import Config
import ezdxf
from pyproj import Transformer
try:
    from supabase import create_client
except ImportError:
    create_client = None

# 환경 변수 로드 (GitHub Secrets에서 주입됨)
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# [추가] 필수 환경 변수 검증 로직
required_vars = {
    "R2_ACCOUNT_ID": R2_ACCOUNT_ID,
    "R2_ACCESS_KEY_ID": R2_ACCESS_KEY_ID,
    "R2_SECRET_ACCESS_KEY": R2_SECRET_ACCESS_KEY,
    "R2_BUCKET_NAME": R2_BUCKET_NAME
}

missing = [key for key, val in required_vars.items() if not val]
if missing:
    print(f"Error: 다음 환경 변수들이 GitHub Secrets에 설정되지 않았습니다: {', '.join(missing)}")
    sys.exit(1)

def get_supabase_client():
    if create_client and SUPABASE_URL and SUPABASE_KEY:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    return None

def get_r2_client():
    return boto3.client(
        's3',
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4')
    )

def download_dxf_from_r2(project_id):
    """R2에서 DXF 파일 다운로드"""
    print(f"Downloading DXF for Project {project_id} from R2...")
    s3 = get_r2_client()
    key = f"cad_data/CAD_{project_id}.dxf"
    
    try:
        s3.download_file(R2_BUCKET_NAME, key, "input.dxf")
        print("DXF Download complete.")
        return True
    except Exception as e:
        print(f"Error downloading DXF: {e}")
        return False

def dxf_to_geojson(source_crs, target_layers):
    """DXF 파일을 GeoJSON으로 변환 (pyproj 좌표계 변환 및 레이어 필터링 적용)"""
    print(f"Converting DXF to GeoJSON (CRS: {source_crs})...")
    print(f"Target Layers: {target_layers}")
    
    try:
        transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)
        doc = ezdxf.readfile("input.dxf")
        msp = doc.modelspace()
        
        features_map = {'Point': [], 'LineString': []}
        stats = {'Point': 0, 'LineString': 0}

        def process_entity(e):
            try:
                if e.dxf.layer not in target_layers: return

                dxftype = e.dxftype()
                if dxftype == 'INSERT':
                    for sub_e in e.virtual_entities(): process_entity(sub_e)
                    return

                if dxftype not in ['TEXT', 'MTEXT', 'POINT', 'CIRCLE', 'LWPOLYLINE', 'LINE', 'POLYLINE', 'ARC', 'SPLINE']: return

                geom_type = None
                coords = []
                props = {"handle": e.dxf.handle, "layer": e.dxf.layer, "dxftype": dxftype}

                if dxftype in ['TEXT', 'MTEXT']:
                    props['text'] = e.dxf.text if dxftype == 'TEXT' else e.text

                # Geometry Conversion
                if dxftype == 'LINE':
                    geom_type = "LineString"
                    p_s, p_e = e.dxf.start, e.dxf.end
                    coords = [transformer.transform(p_s[0], p_s[1]), transformer.transform(p_e[0], p_e[1])]
                elif dxftype == 'LWPOLYLINE':
                    points = list(e.get_points('xy'))
                    if len(points) < 2: return
                    coords = [transformer.transform(p[0], p[1]) for p in points]
                    if e.closed and coords[0] != coords[-1]: coords.append(coords[0])
                    geom_type = "LineString"
                elif dxftype == 'POLYLINE':
                    points = list(e.points())
                    if len(points) < 2: return
                    coords = [transformer.transform(p[0], p[1]) for p in points]
                    if e.is_closed and coords[0] != coords[-1]: coords.append(coords[0])
                    geom_type = "LineString"
                elif dxftype == 'CIRCLE':
                    geom_type = "Point"
                    p = e.dxf.center
                    coords = transformer.transform(p[0], p[1])
                    props['radius'] = e.dxf.radius
                elif dxftype in ['TEXT', 'MTEXT', 'POINT']:
                    geom_type = "Point"
                    p = e.dxf.insert if dxftype in ['TEXT', 'MTEXT'] else e.dxf.location
                    coords = transformer.transform(p[0], p[1])

                if geom_type and coords:
                    feat = {"type": "Feature", "geometry": {"type": geom_type, "coordinates": coords}, "properties": props}
                    features_map[geom_type].append(feat)
                    stats[geom_type] += 1
            except: pass
        
        for e in msp: process_entity(e)
        print(f"Conversion Stats: {stats}")

        if features_map['Point']:
            with open("temp_point.geojson", "w", encoding="utf-8") as f:
                json.dump({"type": "FeatureCollection", "features": features_map['Point']}, f, ensure_ascii=False)
        if features_map['LineString']:
            with open("temp_line.geojson", "w", encoding="utf-8") as f:
                json.dump({"type": "FeatureCollection", "features": features_map['LineString']}, f, ensure_ascii=False)

        return True
    except Exception as e:
        print(f"GeoJSON conversion error: {e}")
        return False

def convert_to_pmtiles():
    """Tippecanoe를 사용하여 GeoJSON을 PMTiles로 변환"""
    print("Converting to PMTiles...")
    
    cmd = [
        "tippecanoe",
        "-o", "output.pmtiles",
        "-zg", 
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        "--force"
    ]
    
    has_input = False
    if os.path.exists("temp_point.geojson"):
        cmd.extend(["-L", "point:temp_point.geojson"])
        has_input = True
    if os.path.exists("temp_line.geojson"):
        cmd.extend(["-L", "line:temp_line.geojson"])
        has_input = True
        
    if not has_input:
        print("No GeoJSON input files found.")
        return False
    
    try:
        subprocess.run(cmd, check=True)
        print("Conversion complete.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Conversion failed: {e}")
        return False

def upload_to_r2(project_id, cache_control):
    """Cloudflare R2에 PMTiles 업로드"""
    print("Uploading to R2...")
    
    s3 = get_r2_client()
    
    file_name = f"cad_data/cad_{project_id}_Data.pmtiles"
    
    try:
        # 기존 파일 삭제 시도
        try: s3.delete_object(Bucket=R2_BUCKET_NAME, Key=file_name)
        except: pass

        with open("output.pmtiles", "rb") as f:
            s3.upload_fileobj(
                f, 
                R2_BUCKET_NAME, 
                file_name,
                ExtraArgs={
                    'ContentType': 'application/vnd.pmtiles',
                    'CacheControl': cache_control
                }
            )
        print(f"Upload success: {file_name}")
        
        # Supabase 메타데이터 업데이트
        supabase = get_supabase_client()
        if supabase:
            try:
                size = os.path.getsize("output.pmtiles")
                data = {
                    "project_id": int(project_id),
                    "file_type": "pmtiles",
                    "file_path": file_name,
                    "file_size": size,
                    "updated_at": "now()"
                }
                # Upsert logic
                res = supabase.table("cad_files").select("id").eq("file_path", file_name).execute()
                if res.data:
                    supabase.table("cad_files").update(data).eq("file_path", file_name).execute()
                else:
                    supabase.table("cad_files").insert(data).execute()
                print("Supabase metadata updated.")
            except Exception as e:
                print(f"Supabase update failed: {e}")

        return True
    except Exception as e:
        print(f"Upload failed: {e}")
        return False

if __name__ == "__main__":
    # 커맨드라인 인자로 JSON 페이로드 받기
    if len(sys.argv) < 2:
        print("Usage: python convert_r2.py <json_payload>")
        sys.exit(1)
        
    try:
        payload = json.loads(sys.argv[1])
        project_id = payload.get('project_id')
        source_crs = payload.get('source_crs', 'EPSG:5187')
        layers = payload.get('layers', [])
        cache_control = payload.get('cache_control', 'no-cache')
        
        print(f"Starting conversion for Project {project_id}")
        
        if download_dxf_from_r2(project_id):
            if dxf_to_geojson(source_crs, layers):
                if convert_to_pmtiles():
                    if upload_to_r2(project_id, cache_control):
                        print("All steps completed successfully.")
                    else:
                        sys.exit(1)
                else:
                    sys.exit(1)
            else:
                sys.exit(1)
        else:
            sys.exit(1)
            
    except json.JSONDecodeError:
        print("Invalid JSON payload")
        sys.exit(1)
