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
    print("✅ Supabase library imported successfully.")
except ImportError as e:
    print(f"❌ Failed to import supabase: {e}")
    create_client = None

# 환경 변수 로드 (GitHub Secrets에서 주입됨)
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID", "").strip()
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "").strip()
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "").strip()
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "").strip()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()

# [추가] 필수 환경 변수 검증 로직
required_vars = {
    "R2_ACCOUNT_ID": R2_ACCOUNT_ID,
    "R2_ACCESS_KEY_ID": R2_ACCESS_KEY_ID,
    "R2_SECRET_ACCESS_KEY": R2_SECRET_ACCESS_KEY,
    "R2_BUCKET_NAME": R2_BUCKET_NAME,
    "SUPABASE_URL": SUPABASE_URL,
    "SUPABASE_KEY": SUPABASE_KEY
}

missing = [key for key, val in required_vars.items() if not val]
if missing:
    print(f"Error: 다음 환경 변수들이 GitHub Secrets에 설정되지 않았습니다: {', '.join(missing)}")
    sys.exit(1)

def get_supabase_client():
    if not create_client:
        print("⚠️ Supabase client creation skipped: Library not imported.")
        return None
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("⚠️ Supabase client creation skipped: Missing URL or KEY.")
        return None
    
    # [디버깅] 설정 확인 (보안을 위해 앞부분만 출력)
    print(f"🔍 Supabase Config Check: URL={SUPABASE_URL[:15]}..., KEY={SUPABASE_KEY[:5]}...{SUPABASE_KEY[-5:]}")
    
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"❌ Supabase client initialization failed: {e}")
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

def download_json_from_r2(project_id):
    """R2에서 JSON 파일 다운로드"""
    print(f"Downloading JSON for Project {project_id} from R2...")
    s3 = get_r2_client()
    key = f"cad_data/CAD_{project_id}.json"
    
    try:
        s3.download_file(R2_BUCKET_NAME, key, "input.json")
        print("JSON Download complete.")
        return True
    except Exception as e:
        print(f"Error downloading JSON: {e}")
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
                # [수정] target_layers가 비어있으면 모든 레이어 처리
                if target_layers and e.dxf.layer not in target_layers: return

                dxftype = e.dxftype()
                if dxftype == 'INSERT':
                    for sub_e in e.virtual_entities(): process_entity(sub_e)
                    return

                if dxftype not in ['TEXT', 'MTEXT', 'POINT', 'CIRCLE', 'LWPOLYLINE', 'LINE', 'POLYLINE', 'ARC', 'SPLINE']: return

                geom_type = None
                coords = []
                props = {"handle": e.dxf.handle, "layer": e.dxf.layer, "dxftype": dxftype}

                # [추가] 색상(ACI) 및 회전(Rotation) 정보 저장
                props['color'] = e.dxf.get('color', 256)  # 256: ByLayer
                if e.dxf.hasattr('rotation'):
                    # DXF는 반시계(CCW), 웹(Mapbox/MapLibre)은 시계(CW) 방향이므로 부호 반전
                    props['rotation'] = -float(e.dxf.rotation)

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

def json_to_supabase_and_geojson(project_id, source_crs):
    """JSON -> Supabase Insert -> GeoJSON Export"""
    print("Processing JSON workflow...")
    supabase = get_supabase_client()
    if not supabase: return False

    try:
        # 1. Load JSON
        with open("input.json", "rb") as f:
            raw_data = f.read()
        try:
            data = json.loads(raw_data.decode('utf-8-sig'))
        except UnicodeDecodeError:
            print("⚠️ UTF-8 decode failed, trying CP949...")
            data = json.loads(raw_data.decode('cp949'))
        
        # 2. Prepare Data for Insert
        insert_rows = []
        for obj in data:
            geom_str = obj.get('wkt')
            if geom_str and not geom_str.startswith('SRID='):
                geom_str = f"SRID=4326;{geom_str}"
            
            row = {
                "project_id": int(project_id),
                "handle": obj.get('handle'),
                "layer": obj.get('layer'),
                "block_name": obj.get('block_name'),
                "text_content": obj.get('text'),
                "x_coord": obj.get('x'),
                "y_coord": obj.get('y'),
                "rotation": obj.get('rotation'),
                "geom": geom_str
            }
            insert_rows.append(row)

        # 3. Delete Old Data & Insert New (Batch)
        print(f"Deleting old data for project {project_id}...")
        supabase.table("cad_objects").delete().eq("project_id", project_id).execute()
        
        print(f"Inserting {len(insert_rows)} rows...")
        batch_size = 1000
        for i in range(0, len(insert_rows), batch_size):
            batch = insert_rows[i:i+batch_size]
            supabase.table("cad_objects").insert(batch).execute()
        
        # 4. Fetch Data as GeoJSON (Using Python conversion for simplicity and reliability)
        # Supabase에서 데이터를 다시 조회하여 GeoJSON 생성 (PostGIS의 정확성 활용)
        print("Fetching data for GeoJSON conversion...")
        
        # 페이지네이션으로 전체 데이터 조회
        all_rows = []
        current = 0
        limit = 1000
        while True:
            res = supabase.table("cad_objects").select("handle, layer, text_content, geom").eq("project_id", project_id).range(current*limit, (current+1)*limit-1).execute()
            if not res.data: break
            all_rows.extend(res.data)
            if len(res.data) < limit: break
            current += 1
            
        features_map = {'Point': [], 'LineString': []}
        
        # WKT 파싱을 위해 shapely 사용 (없으면 간단한 파싱)
        # GitHub Action 환경에는 shapely가 없을 수 있으므로 ezdxf/pyproj 외존성만 사용하거나
        # 여기서는 간단히 WKT 문자열 처리를 수행 (POINT, LINESTRING만 처리)
        
        for row in all_rows:
            geom_val = row['geom']
            if not geom_val: continue
            
            geom_type = None
            coords = []
            
            # [수정] Supabase 반환값이 GeoJSON(dict)인 경우와 WKT(str)인 경우 모두 처리
            if isinstance(geom_val, dict):
                geom_type = geom_val.get('type')
                coords = geom_val.get('coordinates')
            elif isinstance(geom_val, str):
                wkt = geom_val
                if ';' in wkt: wkt = wkt.split(';')[1]
                
                if wkt.startswith("POINT"):
                    geom_type = "Point"
                    content = wkt[6:-1]
                    coords = list(map(float, content.split()))
                elif wkt.startswith("LINESTRING"):
                    geom_type = "LineString"
                    content = wkt[11:-1]
                    coords = [list(map(float, p.strip().split())) for p in content.split(',')]
            
            if geom_type and geom_type in features_map:
                props = {"handle": row['handle'], "layer": row['layer'], "text": row['text_content']}
                feat = {"type": "Feature", "geometry": {"type": geom_type, "coordinates": coords}, "properties": props}
                features_map[geom_type].append(feat)

        if features_map['Point']:
            with open("temp_point.geojson", "w", encoding="utf-8") as f:
                json.dump({"type": "FeatureCollection", "features": features_map['Point']}, f, ensure_ascii=False)
        if features_map['LineString']:
            with open("temp_line.geojson", "w", encoding="utf-8") as f:
                json.dump({"type": "FeatureCollection", "features": features_map['LineString']}, f, ensure_ascii=False)
        
        return True
    except Exception as e:
        print(f"JSON workflow error: {e}")
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
                    # 'ContentType': 'application/vnd.pmtiles', # Colab과 동일하게 자동 설정(또는 없음)으로 변경
                    'CacheControl': cache_control
                }
            )
        print(f"Upload success: {file_name}")
        
        # Supabase 메타데이터 업데이트
        print("🔄 Updating Supabase metadata...")
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
                print(f"❌ Supabase update failed: {e}")
        else:
            print("⚠️ Supabase client is not available. Metadata update skipped.")

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
        input_type = payload.get('input_type', 'dxf') # dxf or json
        
        print(f"Starting conversion for Project {project_id} (Type: {input_type})")
        
        success = False
        
        if input_type == 'json':
            if download_json_from_r2(project_id):
                if json_to_supabase_and_geojson(project_id, source_crs):
                    if convert_to_pmtiles():
                        if upload_to_r2(project_id, cache_control):
                            success = True
        else:
            # Default DXF workflow
            if download_dxf_from_r2(project_id):
                if dxf_to_geojson(source_crs, layers):
                    if convert_to_pmtiles():
                        if upload_to_r2(project_id, cache_control):
                            success = True

        if success:
            print("All steps completed successfully.")
        else:
            sys.exit(1)
            
    except json.JSONDecodeError:
        print("Invalid JSON payload")
        sys.exit(1)
