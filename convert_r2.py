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
    from shapely.geometry import Point, LineString, MultiLineString
    from shapely.ops import linemerge
except ImportError:
    print("⚠️ Shapely library not found. Chainage calculation will be skipped.")
    Point, LineString, MultiLineString, linemerge = None, None, None, None
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

def get_chainage_details(line_geom, pt_geom, total_length, reverse=False):
    """체인리지, 방향, 오프셋 계산"""
    try:
        # 1. Station (시점으로부터의 거리)
        dist = line_geom.project(pt_geom)
        
        # 2. Offset (중심선과의 수직 거리)
        offset = line_geom.distance(pt_geom)
        
        # 3. 방향 (좌/우) 판별
        # 투영점(중심선 상의 점) 구하기
        proj_pt = line_geom.interpolate(dist)
        
        # 접선 벡터 구하기 (진행 방향)
        delta = 0.1
        if dist + delta <= total_length:
            next_pt = line_geom.interpolate(dist + delta)
            vec_line = (next_pt.x - proj_pt.x, next_pt.y - proj_pt.y)
        else:
            prev_pt = line_geom.interpolate(dist - delta)
            vec_line = (proj_pt.x - prev_pt.x, proj_pt.y - prev_pt.y)
            
        # 투영점 -> 대상점 벡터
        vec_pt = (pt_geom.x - proj_pt.x, pt_geom.y - proj_pt.y)
        
        # 외적 (Cross Product)으로 좌우 판별: x1*y2 - x2*y1
        # 진행방향 기준: 양수=좌측, 음수=우측 (일반적인 좌표계)
        cross_prod = vec_line[0] * vec_pt[1] - vec_line[1] * vec_pt[0]
        direction_str = "중앙"
        if cross_prod < 0: direction_str = "우"
        elif cross_prod > 0: direction_str = "좌"
        
        # 역방향 처리 (거리는 반전하되, 상행 기준이므로 좌우/상행 표기는 유지)
        final_dist = total_length - dist if reverse else dist
        
        km = int(final_dist / 1000)
        m = final_dist % 1000
        
        # 요청 포맷: 0+100.76/상행(우)/3.1
        return f"{km}+{m:06.2f}/상행({direction_str})/{offset:.1f}"
    except:
        return None

def dxf_to_geojson_and_db_json(project_id, source_crs, target_layers, centerline_layer=None, reverse_chainage=False, output_formats=None):
    """DXF 파일을 GeoJSON으로 변환 (pyproj 좌표계 변환 및 레이어 필터링 적용)"""
    print(f"Converting DXF to GeoJSON (CRS: {source_crs})...")
    print(f"Target Layers: {target_layers}")
    
    if output_formats is None: output_formats = ['pmtiles', 'json']
    do_db_json = 'json' in output_formats
    do_viz_pmtiles = 'pmtiles' in output_formats
    
    try:
        transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)
        doc = ezdxf.readfile("input.dxf")
        msp = doc.modelspace()

        # [Schema Load]
        schema = {}
        if os.path.exists("cad_schema.json"):
            with open("cad_schema.json", "r", encoding="utf-8") as f:
                schema = json.load(f).get("columns", {})
        
        # [추가] 도로중심선 지오메트리 추출 및 병합 (Shapely 사용)
        centerline_geom = None
        centerline_len = 0
        if centerline_layer and LineString:
            print(f"Processing Centerline Layer: {centerline_layer}")
            lines = []
            # 해당 레이어의 선형 객체만 추출
            cl_entities = msp.query(f'*[layer=="{centerline_layer}"]')
            for e in cl_entities:
                try:
                    if e.dxftype() in ['LINE', 'LWPOLYLINE', 'POLYLINE']:
                        pts = list(e.points()) if e.dxftype() != 'LWPOLYLINE' else list(e.get_points('xy'))
                        if len(pts) >= 2:
                            # 2D 좌표만 사용
                            lines.append(LineString([(p[0], p[1]) for p in pts]))
                except: pass
            
            if lines:
                try:
                    merged = linemerge(lines)
                    centerline_geom = merged
                    centerline_len = merged.length
                    print(f"Centerline constructed. Total Length: {centerline_len:.2f}")
                except Exception as e:
                    print(f"Centerline merge failed: {e}")
        else:
            if not centerline_layer:
                print("ℹ️ No centerline layer provided. Chainage calculation skipped.")
            elif not LineString:
                print("⚠️ Centerline layer provided but Shapely library is missing. Chainage calculation skipped.")
        
        features_map = {'Point': [], 'LineString': []}
        stats = {'Point': 0, 'LineString': 0}
        db_json_list = [] # DB 업로드용 JSON 리스트

        def process_entity(e):
            try:
                # [수정] target_layers가 비어있으면 모든 레이어 처리
                # Worthless_Object 레이어는 target_layers에 없어도 처리 (특수 목적)
                is_worthless = (e.dxf.layer.lower() == "worthless_object")
                if target_layers and not is_worthless and e.dxf.layer not in target_layers: return

                dxftype = e.dxftype()
                if dxftype == 'INSERT':
                    # 블록 내부 형상은 분해하여 재귀 처리
                    for sub_e in e.virtual_entities(): process_entity(sub_e)

                # 블록 자체를 포함하여 허용된 타입만 처리
                if dxftype not in ['TEXT', 'MTEXT', 'POINT', 'CIRCLE', 'LWPOLYLINE', 'LINE', 'POLYLINE', 'ARC', 'SPLINE', 'ELLIPSE', 'INSERT']: return

                geom_type = None
                coords = []
                props = {"handle": e.dxf.handle, "layer": e.dxf.layer, "dxftype": dxftype}

                # 색상(ACI) 및 회전(Rotation) 정보 저장
                props['color'] = e.dxf.get('color', 256)  # 256: ByLayer
                if e.dxf.hasattr('rotation'):
                    # DXF는 반시계(CCW), 웹(Mapbox/MapLibre)은 시계(CW) 방향이므로 부호 반전
                    props['rotation'] = -float(e.dxf.rotation)

                chainage_val = None
                # 원본 TM 좌표 및 체인리지 계산
                tm_pt = None
                if dxftype in ['TEXT', 'MTEXT', 'INSERT']:
                    tm_pt = e.dxf.insert
                elif dxftype == 'POINT':
                    tm_pt = e.dxf.location
                elif dxftype == 'CIRCLE':
                    tm_pt = e.dxf.center
                elif dxftype == 'LINE':
                    tm_pt = e.dxf.start # 선은 시작점 기준
                
                if tm_pt:
                    props['tm_x'] = round(tm_pt[0], 3)
                    props['tm_y'] = round(tm_pt[1], 3)
                    
                    # 체인리지 계산 (Shapely Project)
                    if centerline_geom:
                        try:
                            pt = Point(tm_pt[0], tm_pt[1])
                            c_info = get_chainage_details(centerline_geom, pt, centerline_len, reverse_chainage)
                            if c_info:
                                props['chainage'] = c_info
                            chainage_val = props['chainage']
                        except: pass

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
                elif dxftype in ['TEXT', 'MTEXT', 'POINT', 'INSERT']:
                    geom_type = "Point"
                    p = e.dxf.insert if dxftype in ['TEXT', 'MTEXT', 'INSERT'] else e.dxf.location
                    coords = transformer.transform(p[0], p[1])
                elif dxftype in ['ARC', 'SPLINE', 'ELLIPSE']:
                    try:
                        points = list(e.flattening(0.001))
                        if len(points) >= 2:
                            coords = [transformer.transform(p[0], p[1]) for p in points]
                            geom_type = "LineString"
                    except: pass

                if geom_type and coords:
                    if do_viz_pmtiles:
                        feat = {"type": "Feature", "geometry": {"type": geom_type, "coordinates": coords}, "properties": props}
                        features_map[geom_type].append(feat)
                        stats[geom_type] += 1

                    # [DB JSON 생성] 스키마 기반 매핑
                    if do_db_json and schema:
                        db_item = {}
                        dxf_attrs = {
                            "handle": e.dxf.handle,
                            "layer": e.dxf.layer,
                            "block_name": e.dxf.name if dxftype == 'INSERT' else None,
                            "text": props.get('text'),
                            "x": props.get('tm_x'),
                            "y": props.get('tm_y'),
                            "rotation": props.get('rotation', 0)
                        }
                        
                        for col, rule in schema.items():
                            src = rule.get("source")
                            if src == "dxf":
                                db_item[col] = dxf_attrs.get(rule.get("attr"))
                            elif src == "calc":
                                method = rule.get("method")
                                if method == "chainage":
                                    db_item[col] = chainage_val
                                elif method == "wkt":
                                    if geom_type == "Point":
                                        db_item[col] = f"SRID=4326;POINT({coords[0]} {coords[1]})"
                                    elif geom_type == "LineString":
                                        pairs = ", ".join([f"{c[0]} {c[1]}" for c in coords])
                                        db_item[col] = f"SRID=4326;LINESTRING({pairs})"
                        
                        db_json_list.append(db_item)

            except: pass
        
        for e in msp: process_entity(e)
        print(f"Conversion Stats: {stats}")

        if do_viz_pmtiles and features_map['Point']:
            with open("temp_point.geojson", "w", encoding="utf-8") as f:
                json.dump({"type": "FeatureCollection", "features": features_map['Point']}, f, ensure_ascii=False)
        if do_viz_pmtiles and features_map['LineString']:
            with open("temp_line.geojson", "w", encoding="utf-8") as f:
                json.dump({"type": "FeatureCollection", "features": features_map['LineString']}, f, ensure_ascii=False)

        # [DB용 JSON 저장]
        if do_db_json and db_json_list:
            with open(f"CAD_{project_id}.json", "w", encoding="utf-8") as f:
                json.dump(db_json_list, f, ensure_ascii=False)

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
        transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)
    except Exception as e:
        print(f"❌ Invalid CRS {source_crs}: {e}")
        return False

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
            x = obj.get('x')
            y = obj.get('y')
            wkt = obj.get('wkt')
            
            tx, ty = None, None
            final_wkt = None
            
            if x is not None and y is not None:
                try:
                    tx, ty = transformer.transform(float(x), float(y))
                except: pass
            
            if wkt:
                clean_wkt = wkt.split(';')[-1] if ';' in wkt else wkt
                clean_wkt = clean_wkt.strip().upper()
                try:
                    if clean_wkt.startswith("POINT"):
                        content = clean_wkt[clean_wkt.find("(")+1 : clean_wkt.find(")")]
                        parts = content.split()
                        if len(parts) >= 2:
                            px, py = float(parts[0]), float(parts[1])
                            tpx, tpy = transformer.transform(px, py)
                            final_wkt = f"SRID=4326;POINT({tpx} {tpy})"
                            if tx is None: tx, ty = tpx, tpy
                    elif clean_wkt.startswith("LINESTRING"):
                        content = clean_wkt[clean_wkt.find("(")+1 : clean_wkt.find(")")]
                        pairs = content.split(',')
                        new_pairs = []
                        for pair in pairs:
                            parts = pair.strip().split()
                            if len(parts) >= 2:
                                px, py = float(parts[0]), float(parts[1])
                                tpx, tpy = transformer.transform(px, py)
                                new_pairs.append(f"{tpx} {tpy}")
                        if new_pairs:
                            final_wkt = f"SRID=4326;LINESTRING({', '.join(new_pairs)})"
                except: pass
            
            if not final_wkt:
                if tx is not None and ty is not None:
                    final_wkt = f"SRID=4326;POINT({tx} {ty})"
                elif wkt:
                    final_wkt = f"SRID=4326;{clean_wkt}"
            
            row = {
                "project_id": int(project_id),
                "handle": obj.get('handle'),
                "layer": obj.get('layer'),
                "block_name": obj.get('block_name'),
                "text_content": obj.get('text'),
                "x_coord": tx if tx is not None else x,
                "y_coord": ty if ty is not None else y,
                "rotation": obj.get('rotation'),
                "chainage": obj.get('chainage'),
                "geom": final_wkt
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
        
        # 4. Fetch Data as GeoJSON
        print("Fetching data and generating GeoJSON...")
        
        all_rows = []
        current = 0
        limit = 1000
        while True:
            res = supabase.table("cad_objects").select("handle, layer, text_content, geom, rotation, chainage").eq("project_id", project_id).range(current*limit, (current+1)*limit-1).execute()
            if not res.data: break
            all_rows.extend(res.data)
            if len(res.data) < limit: break
            current += 1
            
        features_map = {'Point': [], 'LineString': []}
        
        for row in all_rows:
            geom_val = row['geom']
            if not geom_val: continue
            
            geom_type = None
            coords = []
            
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
                if row.get('rotation'):
                    props['rotation'] = -float(row['rotation'])
                if row.get('chainage'):
                    props['chainage'] = row['chainage']
                
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
        "-z22",
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        "--force",
        "--no-line-simplification",
        "--no-tiny-polygon-reduction",
        "-r1.5"
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
    """Cloudflare R2에 PMTiles 및 JSON 업로드"""
    print("Uploading to R2...")
    
    s3 = get_r2_client()
    supabase = get_supabase_client() # [수정] Supabase 클라이언트 가져오기

    # [수정] 업로드할 파일과 메타데이터를 리스트로 관리
    files_to_upload = []
    
    # 1. PMTiles 파일 정보
    if os.path.exists("output.pmtiles"):
        files_to_upload.append({
            "local_path": "output.pmtiles",
            "r2_key": f"cad_data/cad_{project_id}_Data.pmtiles",
            "file_type": "pmtiles"
        })
        
    # 2. DB용 JSON 파일 정보
    json_file_local = f"CAD_{project_id}.json"
    if os.path.exists(json_file_local):
        files_to_upload.append({
            "local_path": json_file_local,
            "r2_key": f"cad_data/CAD_{project_id}.json",
            "file_type": "json"
        })

    if not files_to_upload:
        print("No files to upload.")
        return False

    try:
        for file_info in files_to_upload:
            local_path = file_info["local_path"]
            r2_key = file_info["r2_key"]
            file_type = file_info["file_type"]

            print(f"Uploading {local_path} to {r2_key}...")

            # 기존 파일 삭제 시도
            try: s3.delete_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
            except: pass

            # 파일 업로드
            with open(local_path, "rb") as f:
                s3.upload_fileobj(f, R2_BUCKET_NAME, r2_key, ExtraArgs={'CacheControl': cache_control} if file_type == 'pmtiles' else {})
            print(f"  -> Upload success: {r2_key}")

            # Supabase 메타데이터 업데이트
            if supabase:
                print(f"  -> Updating Supabase metadata for {file_type}...")
                try:
                    size = os.path.getsize(local_path)
                    data = {"project_id": int(project_id), "file_type": file_type, "file_path": r2_key, "file_size": size, "updated_at": "now()"}
                    res = supabase.table("cad_files").select("id").eq("file_path", r2_key).execute()
                    if res.data: supabase.table("cad_files").update(data).eq("file_path", r2_key).execute()
                    else: supabase.table("cad_files").insert(data).execute()
                    print("  -> Supabase metadata updated.")
                except Exception as e: print(f"  -> ❌ Supabase update failed: {e}")
            else: print("  -> ⚠️ Supabase client not available. Metadata update skipped.")
        return True
    except Exception as e:
        print(f"Upload process failed: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python convert_r2.py <json_payload>")
        sys.exit(1)
        
    try:
        payload = json.loads(sys.argv[1])
        project_id = payload.get('project_id')
        source_crs = payload.get('source_crs', 'EPSG:5187')
        layers = payload.get('layers', [])
        cache_control = payload.get('cache_control', 'no-cache')
        centerline_layer = payload.get('centerline_layer')
        reverse_chainage = payload.get('reverse_chainage', False)
        input_type = payload.get('input_type', 'dxf')
        output_formats = payload.get('output_formats', ['pmtiles', 'json'])
        
        print(f"Starting conversion for Project {project_id} (Type: {input_type})")
        
        success = False
        
        if input_type == 'json':
            if download_json_from_r2(project_id):
                if json_to_supabase_and_geojson(project_id, source_crs):
                    # [수정] PMTiles 변환 조건부 실행
                    pmtiles_success = True
                    if 'pmtiles' in output_formats:
                        pmtiles_success = convert_to_pmtiles()
                    
                    if pmtiles_success:
                        if upload_to_r2(project_id, cache_control):
                            success = True
        else:
            if download_dxf_from_r2(project_id):
                if dxf_to_geojson_and_db_json(project_id, source_crs, layers, centerline_layer, reverse_chainage, output_formats):
                    # [수정] PMTiles 변환 조건부 실행
                    pmtiles_success = True
                    if 'pmtiles' in output_formats:
                        pmtiles_success = convert_to_pmtiles()
                    
                    if pmtiles_success:
                        if upload_to_r2(project_id, cache_control):
                            success = True

        if success:
            print("All steps completed successfully.")
        else:
            sys.exit(1)
            
    except json.JSONDecodeError:
        print("Invalid JSON payload")
        sys.exit(1)
