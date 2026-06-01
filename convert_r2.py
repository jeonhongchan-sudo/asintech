import os
import sys
import json
import re
import math
import subprocess
import zipfile
import boto3
import requests
from datetime import datetime, timedelta, timezone
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

def download_from_r2(key, local_path):
    """R2에서 파일 다운로드 (공통)"""
    print(f"Downloading {key} to {local_path} from R2...")
    s3 = get_r2_client()
    
    try:
        s3.download_file(R2_BUCKET_NAME, key, local_path)
        print(f"Download complete: {local_path}")
        return True
    except Exception as e:
        print(f"Error downloading {key}: {e}")
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
        
        # [설정] 정밀도 전략: 계산은 고정밀(Double)로 유지하고, 최종 출력 시 소수점 3자리(mm)로 반올림
        # 포맷: 0+000.000 (전체 7자리, 소수점 3자리)
        return f"{km}+{m:07.3f}/상행({direction_str})/{offset:.3f}"
    except:
        return None
def dxf_to_geojson(project_id, source_crs, target_layers, centerline_layer=None, reverse_chainage=False):
    """DXF 파일을 GeoJSON으로 변환 (pyproj 좌표계 변환 및 레이어 필터링 적용)"""
    print(f"Converting DXF to GeoJSON (CRS: {source_crs})...")
    print(f"Target Layers: {target_layers}")    
    
    # [추가] 원본 SRID 추출 (예: "EPSG:5187" -> "5187")
    srid = source_crs.split(':')[-1] if ':' in source_crs else '4326'

    try:
        transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)
        doc = ezdxf.readfile("input.dxf")
        msp = doc.modelspace()
        print(f"DXF Loaded. Entities in Modelspace: {len(msp)}")
        
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
            if not centerline_layer: print("ℹ️ No centerline layer provided. Chainage calculation skipped.")
            elif not LineString: 
                print("⚠️ Centerline layer provided but Shapely library is missing. Chainage calculation skipped.")
        
        features_map = {'Point': [], 'LineString': [], 'Polygon': []}
        stats = {'Point': 0, 'LineString': 0, 'Polygon': 0}

        def process_entity(e, is_inside_block=False):
            try:
                # [복구] 과거의 안정적인 레이어 필터링 방식
                if target_layers and e.dxf.layer not in target_layers: return

                dxftype = e.dxftype()

                # [NEW] Special handling for polylines with width for visualization
                if dxftype in ['LWPOLYLINE', 'POLYLINE']:
                    segments = []
                    # LWPOLYLINE: 각 정점의 start_width, end_width 정보를 가져옴
                    if dxftype == 'LWPOLYLINE' and e.has_width:
                        # xyseb: x, y, start_width, end_width, bulge
                        pts = list(e.get_points('xyseb'))
                        for i in range(len(pts) - 1):
                            segments.append({
                                'p1': (pts[i][0], pts[i][1]),
                                'p2': (pts[i+1][0], pts[i+1][1]),
                                'w1': pts[i][2],
                                'w2': pts[i][3]
                            })
                    # POLYLINE: 각 버텍스의 속성에서 폭 정보를 가져옴
                    elif dxftype == 'POLYLINE':
                        verts = list(e.vertices)
                        # 전체 중 하나라도 폭이 있는 경우 처리
                        if any(v.dxf.start_width > 0 or v.dxf.end_width > 0 for v in verts):
                            for i in range(len(verts) - 1):
                                segments.append({
                                    'p1': verts[i].dxf.location[:2],
                                    'p2': verts[i+1].dxf.location[:2],
                                    'w1': verts[i].dxf.start_width,
                                    'w2': verts[i].dxf.end_width
                                })

                    if segments:
                        processed_as_polygon = False
                        for seg in segments:
                            w1, w2 = seg['w1'], seg['w2']
                            # 폭이 없는 구간은 무시 (또는 선으로 처리하고 싶다면 continue 대신 별도 로직 필요)
                            if w1 <= 0 and w2 <= 0: continue

                            p1, p2 = seg['p1'], seg['p2']
                            dx, dy = p2[0] - p1[0], p2[1] - p1[1]
                            length = math.hypot(dx, dy)
                            if length == 0: continue
                            
                            # 법선 벡터 계산 (Normal Vector)
                            nx, ny = -dy / length, dx / length
                            
                            # 사각형(Trapezoid)의 4개 코너 좌표 계산
                            v = [
                                (p1[0] + nx * w1 / 2, p1[1] + ny * w1 / 2), # Start-Left
                                (p2[0] + nx * w2 / 2, p2[1] + ny * w2 / 2), # End-Left
                                (p2[0] - nx * w2 / 2, p2[1] - ny * w2 / 2), # End-Right
                                (p1[0] - nx * w1 / 2, p1[1] - ny * w1 / 2)  # Start-Right
                            ]
                            
                            poly_coords = [transformer.transform(pt[0], pt[1]) for pt in v]
                            poly_coords.append(poly_coords[0]) # 링 닫기

                            poly_props = {"handle": e.dxf.handle, "layer": e.dxf.layer, "dxftype": f"Polygon_from_{dxftype}"}
                            poly_props['color'] = e.dxf.get('color', 256)

                            feat = {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [poly_coords]}, "properties": poly_props}
                            features_map['Polygon'].append(feat)
                            stats['Polygon'] += 1
                            processed_as_polygon = True
                        
                        # 하나라도 폴리곤으로 변환되었다면, 이 객체는 LineString으로 중복 변환하지 않음
                        if processed_as_polygon: return

                # [PMTiles] 블록(INSERT) 시각화: 분해하여 내부 객체 처리 (재귀)
                if dxftype == 'INSERT':
                    for sub_e in e.virtual_entities(): process_entity(sub_e, is_inside_block=True)

                # 블록 자체를 포함하여 허용된 타입만 처리
                if dxftype not in ['TEXT', 'MTEXT', 'POINT', 'CIRCLE', 'LWPOLYLINE', 'LINE', 'POLYLINE', 'ARC', 'SPLINE', 'ELLIPSE', 'INSERT']: return

                geom_type = None
                coords = []
                orig_coords = [] # [추가] 원본 좌표계 좌표 저장용
                props = {"handle": e.dxf.handle, "layer": e.dxf.layer, "dxftype": dxftype}
                
                # [추가] 텍스트 정렬 정보 추출
                if dxftype == 'TEXT':
                    props['align_h'] = e.dxf.halign
                    props['align_v'] = e.dxf.valign

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
                    # [수정] 계산 정밀도를 위해 불필요한 반올림 제거 (DB 저장 시에는 자동 처리됨)
                    props['tm_x'] = tm_pt[0]
                    props['tm_y'] = tm_pt[1]
                    
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
                    orig_coords = [[p_s[0], p_s[1]], [p_e[0], p_e[1]]]
                elif dxftype == 'LWPOLYLINE':
                    points = list(e.get_points('xy'))
                    if len(points) < 2: return
                    coords = [transformer.transform(p[0], p[1]) for p in points]
                    orig_coords = [[p[0], p[1]] for p in points]
                    if e.closed and coords[0] != coords[-1]: coords.append(coords[0])
                    if e.closed and orig_coords[0] != orig_coords[-1]: orig_coords.append(orig_coords[0])
                    geom_type = "LineString"
                elif dxftype == 'POLYLINE':
                    points = list(e.points())
                    if len(points) < 2: return
                    coords = [transformer.transform(p[0], p[1]) for p in points]
                    orig_coords = [[p[0], p[1]] for p in points]
                    if e.is_closed and coords[0] != coords[-1]: coords.append(coords[0])
                    if e.is_closed and orig_coords[0] != orig_coords[-1]: orig_coords.append(orig_coords[0])
                    geom_type = "LineString"
                elif dxftype == 'CIRCLE':
                    geom_type = "Point"
                    p = e.dxf.center
                    coords = transformer.transform(p[0], p[1])
                    orig_coords = [p[0], p[1]]
                    props['radius'] = e.dxf.radius
                elif dxftype in ['TEXT', 'MTEXT', 'POINT', 'INSERT']:
                    geom_type = "Point"
                    p = e.dxf.insert if dxftype in ['TEXT', 'MTEXT', 'INSERT'] else e.dxf.location
                    coords = transformer.transform(p[0], p[1])
                    orig_coords = [p[0], p[1]]
                elif dxftype in ['ARC', 'SPLINE', 'ELLIPSE']:
                    try:
                        points = list(e.flattening(0.001))
                        if len(points) >= 2:
                            coords = [transformer.transform(p[0], p[1]) for p in points]
                            orig_coords = [[p[0], p[1]] for p in points]
                            geom_type = "LineString"
                    except: pass

                if geom_type and coords:
                    # INSERT 자체는 시각화 데이터(GeoJSON)에 넣지 않음 (분해된 내부 객체만 넣음)
                    if dxftype != 'INSERT':
                        feat = {"type": "Feature", "geometry": {"type": geom_type, "coordinates": coords}, "properties": props}
                        features_map[geom_type].append(feat)
                        stats[geom_type] += 1

            except: pass
        
        for e in msp: process_entity(e)

        # [추가] R2 보관용 통합 GeoJSON 생성 (모든 레이어 통합)
        combined_features = features_map['Point'] + features_map['LineString'] + features_map['Polygon']
        if combined_features:
            with open("temp_combined.geojson", "w", encoding="utf-8") as f:
                json.dump({"type": "FeatureCollection", "features": combined_features}, f, ensure_ascii=False)

        if features_map['Polygon']:
            with open("temp_polygon.geojson", "w", encoding="utf-8") as f: json.dump({"type": "FeatureCollection", "features": features_map['Polygon']}, f, ensure_ascii=False)
        if features_map['Point']:
            with open("temp_point.geojson", "w", encoding="utf-8") as f: json.dump({"type": "FeatureCollection", "features": features_map['Point']}, f, ensure_ascii=False)
        if features_map['LineString']:
            with open("temp_line.geojson", "w", encoding="utf-8") as f: json.dump({"type": "FeatureCollection", "features": features_map['LineString']}, f, ensure_ascii=False)

        return True
    except Exception as e:
        print(f"GeoJSON conversion error: {e}")
        return False

def convert_spatial_to_geojson(input_path, source_crs):
    """ogr2ogr를 사용하여 공간 데이터(SHP, GeoJSON 등)를 GeoJSON(EPSG:4326)으로 변환"""
    print(f"Converting spatial data {input_path} to GeoJSON (Source CRS: {source_crs})...")
    try:
        output_path = "temp_combined.geojson"
        # Tippecanoe 변환을 위해 타겟 좌표계를 EPSG:4326으로 고정
        cmd = ["ogr2ogr", "-f", "GeoJSON", "-t_srs", "EPSG:4326", output_path, input_path]
        
        # 원본 좌표계가 명시된 경우 추가
        if source_crs:
            cmd.extend(["-s_srs", source_crs])
            
        subprocess.run(cmd, check=True)
        return os.path.exists(output_path)
    except Exception as e:
        print(f"GDAL conversion error: {e}")
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
        "-r1" # 포인트 누락 방지
    ]
    
    has_input = False
    if os.path.exists("temp_polygon.geojson"):
        cmd.extend(["-L", "polygon:temp_polygon.geojson"])
        has_input = True
    if os.path.exists("temp_point.geojson"):
        cmd.extend(["-L", "point:temp_point.geojson"])
        has_input = True
    if os.path.exists("temp_line.geojson"):
        cmd.extend(["-L", "line:temp_line.geojson"])
        has_input = True
    
    # SHP/GeoJSON 입력으로 생성된 통합 파일 처리 (DXF가 아닐 때)
    if not has_input and os.path.exists("temp_combined.geojson"):
        cmd.extend(["-L", "data:temp_combined.geojson"])
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

def upload_to_r2(project_id, cache_control, source_crs):
    """Cloudflare R2에 PMTiles 업로드 및 메타데이터 갱신"""
    print("Uploading to R2...")
    
    s3 = get_r2_client()
    supabase = get_supabase_client() # [수정] Supabase 클라이언트 가져오기

    # [추가] 캐시 만료 시간 계산 (DB 업데이트용)
    expiry_iso = None
    if cache_control and "max-age=" in cache_control:
        try:
            # 예: "public, max-age=31536000" -> 31536000 추출
            parts = cache_control.split("max-age=")
            if len(parts) > 1:
                # 숫자 뒤에 콤마나 세미콜론이 올 수 있으므로 처리
                seconds_str = parts[1].split(",")[0].split(";")[0].strip()
                seconds = int(seconds_str)
                if seconds > 0:
                    expiry_dt = datetime.now(timezone.utc) + timedelta(seconds=seconds)
                    expiry_iso = expiry_dt.isoformat()
        except Exception as e:
            print(f"⚠️ Cache expiry calculation failed: {e}")

    # [수정] 업로드할 파일과 메타데이터를 리스트로 관리
    files_to_upload = []
    
    if os.path.exists("output.pmtiles"):
        files_to_upload.append({
            "local_path": "output.pmtiles",
            "r2_key": f"cad_data/cad_{project_id}_Data.pmtiles",
            "file_type": "pmtiles"
        })

    # [추가] 통합된 단일 GeoJSON 파일만 업로드 목록에 추가
    if os.path.exists("temp_combined.geojson"):
        files_to_upload.append({
            "local_path": "temp_combined.geojson",
            "r2_key": f"cad_data/CAD_{project_id}.geojson",
            "file_type": "geojson"
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
                # [수정] 모든 파일에 캐시 설정 적용 (기존에는 PMTiles만 적용되었음)
                s3.upload_fileobj(f, R2_BUCKET_NAME, r2_key, ExtraArgs={'CacheControl': cache_control})
            print(f"  -> Upload success: {r2_key}")

            # Supabase 메타데이터 업데이트
            if supabase:
                print(f"  -> Updating Supabase metadata for {file_type}...")
                try:
                    size = os.path.getsize(local_path)
                    data = {
                        "project_id": int(project_id), 
                        "file_type": file_type, 
                        "file_path": r2_key, 
                        "file_size": size,
                        "source_crs": source_crs,
                        "updated_at": "now()"
                    }
                    # [추가] 캐시 만료 정보가 있으면 업데이트 데이터에 포함
                    if expiry_iso:
                        data["cache_expiry"] = expiry_iso
                    
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

def sanitize_cad_text(s):
    if not s: return ""
    s = str(s).lower()
    s = re.sub(r'\s+', '', s)
    s = re.sub(r'%%[cdp]', '', s, flags=re.IGNORECASE)
    s = re.sub(r'[/\\-_.\(\)\[\]]', '', s)
    return s

def run_recalculation(project_id, dxf_path):
    """도면 갱신 시 기존 계산 정보(연장, 수량 등)를 자동으로 재계산하여 Supabase 업데이트"""
    supabase = get_supabase_client()
    if not supabase:
        print("⚠️ Supabase client not available for recalculation.")
        return

    print(f"Starting automatic recalculation for Project {project_id}...")
    
    try:
        # 1. 현재 저장된 프로젝트 정보 가져오기
        res = supabase.table("project_details").select("*").eq("project_id", project_id).execute()
        if not res.data:
            print("  -> No project details found to update.")
            return
        
        details = res.data[0]
        doc = ezdxf.readfile(dxf_path)
        msp = doc.modelspace()
        print(f"  -> Recalculation: DXF Loaded ({len(msp)} entities in modelspace).")

        # --- A. 관로 정보 재계산 ---
        pipe_info = details.get('pipe_info', {})
        p_headers = pipe_info.get('headers', [])
        p_data = pipe_info.get('data', [])
        idx_len = next((i for i, h in enumerate(p_headers) if h and "연장" in str(h)), -1) # '연장' 헤더 인덱스
        idx_meta = next((i for i, h in enumerate(p_headers) if h == "_layers"), -1) # '_layers' 헤더 인덱스

        print(f"  [Pipe Analysis] Headers found: {p_headers}")
        print(f"  [Pipe Analysis] LenIdx: {idx_len}, MetaIdx: {idx_meta}")
        
        if idx_len != -1 and idx_meta != -1:
            total_m = 0.0
            for i, row in enumerate(p_data):
                row = list(row)  # 수정 가능하도록 리스트 변환
                layers = []
                if 0 <= idx_meta < len(row): # idx_meta가 row의 유효한 인덱스인지 확인
                    layers = [l.strip().upper() for l in str(row[idx_meta]).split(',') if l.strip()]
                
                if not layers:
                    print(f"    - Row {i}: No specific layers to recalculate. Keeping value: {row[idx_len]}")
                    try: total_m += float(row[idx_len])
                    except: pass
                    p_data[i] = row
                    continue
                
                row_len = 0.0
                found_count = 0
                processed_handles = set()
                processed_geometries = set() # 중복 객체 제거(Overkill) 로직
                linear_types = ('LINE', 'LWPOLYLINE', 'POLYLINE', 'ARC', 'SPLINE', 'CIRCLE', 'ELLIPSE')
                
                for e in msp:
                    if not e.is_alive or e.dxf.layer.upper() not in layers or e.dxf.invisible: continue
                    if e.dxf.handle in processed_handles: continue
                    processed_handles.add(e.dxf.handle)
                    
                    etype = e.dxftype()
                    if etype not in linear_types: continue
                    
                    # 기하학적 중복 체크
                    geo_key = None
                    try:
                        if etype == 'LINE':
                            pts = sorted([(round(e.dxf.start.x, 3), round(e.dxf.start.y, 3)), (round(e.dxf.end.x, 3), round(e.dxf.end.y, 3))])
                            geo_key = ("LINE", tuple(pts))
                        elif etype == 'CIRCLE':
                            geo_key = ("CIRCLE", (round(e.dxf.center.x, 3), round(e.dxf.center.y, 3)), round(e.dxf.radius, 3))
                        elif etype == 'LWPOLYLINE':
                            geo_key = ("LWPOLYLINE", tuple((round(p[0], 3), round(p[1], 3)) for p in e.get_points()), e.closed)
                    except: pass
                    
                    if geo_key:
                        if geo_key in processed_geometries: continue
                        processed_geometries.add(geo_key)

                    if etype == 'LINE': row_len += e.dxf.start.distance(e.dxf.end)
                    elif etype == 'CIRCLE': row_len += 2 * math.pi * e.dxf.radius
                    elif hasattr(e, 'length'): row_len += e.length
                    elif etype in ('LWPOLYLINE', 'POLYLINE'):
                        # [수정] 인덱스 변수 i가 상위 반복문의 i(행 번호)를 덮어쓰지 않도록 idx_v로 변경
                        pts = list(e.get_points()) if etype == 'LWPOLYLINE' else [v.dxf.location for v in e.vertices]
                        for idx_v in range(len(pts)-1):
                            p1, p2 = pts[idx_v], pts[idx_v+1]
                            row_len += ((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)**0.5
                        found_count += 1
                
                row[idx_len] = f"{row_len:.2f}"
                total_m += row_len
                p_data[i] = row
                print(f"    - Row {i} Updated: {row_len:.2f}m (Found {found_count} items in {layers})")
            pipe_info['total'] = f"{total_m / 1000.0:.2f}" # km 단위 저장
            print(f"  [Pipe] Recalculation finished. Total: {pipe_info['total']} km")
        else:
            print("  [Pipe] Recalculation skipped: Missing '연장' or '_layers' in headers.")

        # --- B. 맨홀 정보 재계산 ---
        man_info = details.get('manholes_info', {})
        m_headers = man_info.get('headers', [])
        m_data = man_info.get('data', [])
        idx_qty = next((i for i, h in enumerate(m_headers) if h and "수량" in str(h)), -1) # '수량' 헤더 인덱스
        idx_meta = next((i for i, h in enumerate(m_headers) if h == "_layers"), -1) # '_layers' 헤더 인덱스
        
        print(f"  [Manhole Analysis] QtyIdx: {idx_qty}, MetaIdx: {idx_meta}")
        
        if idx_qty != -1 and idx_meta != -1:
            total_man = 0
            for i, row in enumerate(m_data):
                row = list(row)
                layers = []
                if 0 <= idx_meta < len(row): # idx_meta가 row의 유효한 인덱스인지 확인
                    layers = [l.strip().upper() for l in str(row[idx_meta]).split(',') if l.strip()]
                
                if not layers:
                    print(f"    - Row {i}: No layers to recalculate. Skipping Manhole calc.")
                    try: total_man += int(float(row[idx_qty]))
                    except: pass
                    m_data[i] = row
                    continue
                row_qty = 0
                processed_handles = set()
                for e in msp:
                    if e.dxf.layer.upper() in layers and e.dxf.handle not in processed_handles:
                        processed_handles.add(e.dxf.handle)
                        row_qty += 1
                row[idx_qty] = str(row_qty)
                total_man += row_qty
                m_data[i] = row
                print(f"    - Row {i} Updated: {row_qty} (Layers: {layers})")
            man_info['total'] = str(total_man)
            print(f"  [Manhole] Recalculation finished. Total: {man_info['total']}")

        # --- C. 시설물 정보 재계산 ---
        fac_info = details.get('facilities_info', {})
        f_headers = fac_info.get('headers', [])
        f_data = fac_info.get('data', [])
        f_syns = fac_info.get('synonyms', [])
        f_nd = fac_info.get('needs_diam', [])
        f_excls_list = fac_info.get('exclusions', [])

        idx_f_name, idx_f_qty = 0, next((i for i, h in enumerate(f_headers) if h and "수량" in str(h)), -1) # '수량' 헤더 인덱스
        idx_f_diam = next((i for i, h in enumerate(f_headers) if h and "관경" in str(h)), -1) # '관경' 헤더 인덱스
        idx_f_meta = next((i for i, h in enumerate(f_headers) if h == "_layers"), -1) # '_layers' 헤더 인덱스
        
        if idx_f_qty != -1:
            # [수정] 블록 내부 텍스트까지 포함하여 재계산 (동기화)
            all_entities = list(msp.query('TEXT MTEXT'))
            for insert in msp.query('INSERT'):
                all_entities.extend([e for e in insert.virtual_entities() if e.dxftype() in ('TEXT', 'MTEXT')])
            
            # 텍스트와 레이어 정보를 쌍으로 저장
            all_text_data = []
            for e in all_entities:
                txt = e.dxf.text if e.dxftype() == 'TEXT' else (e.plain_text() if hasattr(e, 'plain_text') else e.text)
                all_text_data.append((sanitize_cad_text(txt), e.dxf.layer.upper()))
            
            for i, row in enumerate(f_data):
                row = list(row)
                name = str(row[idx_f_name]).strip()
                diam = str(row[idx_f_diam]).strip() if idx_f_diam != -1 else ""
                
                # 해당 행의 설정값 가져오기
                syn_str = f_syns[i] if i < len(f_syns) else ""
                # [수정] DB의 null(None) 값 대응 및 UI 로직(default=True)과 동기화
                val_nd = f_nd[i] if i < len(f_nd) else True
                needs_diam = True if val_nd is None else bool(val_nd)
                excl_str = f_excls_list[i] if i < len(f_excls_list) else "하단"

                # 검색어 및 제외어 리스트 구성
                keywords = [name] + [s.strip() for s in syn_str.split(',') if s.strip()]
                exclusions = [ex.strip() for ex in excl_str.split(',') if ex.strip()]
                if "하단" not in exclusions: exclusions.append("하단")
                
                # 레이어 필터 확인
                layers = []
                if idx_f_meta != -1 and idx_f_meta < len(row):
                    layers = [l.strip().upper() for l in str(row[idx_f_meta]).split(',') if l.strip()]
                
                s_excls = [sanitize_cad_text(ex) for ex in exclusions]
                
                # [수정] 검색 패턴 생성 로직을 UI와 완벽하게 동기화 (이름+관경 결합)
                if needs_diam and diam:
                    search_patterns = [sanitize_cad_text(kw + diam) for kw in keywords]
                else:
                    search_patterns = [sanitize_cad_text(kw) for kw in keywords]
                
                if keywords[0]: # 기본 시설물명이 있는 경우만 진행
                    count = 0
                    for t_val, t_layer in all_text_data:
                        # 레이어 필터가 있으면 레이어 체크, 없으면 전체 통과
                        if layers and t_layer not in layers: continue
                        
                        # [검색 논리] 1. 제외어 체크 (하나라도 포함되면 제외)
                        if any(ex in t_val for ex in s_excls): continue
                        
                        # [검색 논리] 2. 이름(+관경) 패턴 매칭
                        if any(pat in t_val for pat in search_patterns):
                            count += 1
                    row[idx_f_qty] = str(count)
                f_data[i] = row

        # 3. Supabase 업데이트
        update_payload = {
            "pipe_info": pipe_info,
            "manholes_info": man_info,
            "facilities_info": fac_info,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        res = supabase.table("project_details").update(update_payload).eq("project_id", project_id).execute()
        if res.data:
            print("  -> Recalculation and Supabase update complete.")
        else:
            print(f"  -> ⚠️ Supabase update failed: No rows matched project_id {project_id}")
    except Exception as e:
        print(f"  -> ❌ Recalculation failed: {e}")

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
        
        conversion_ready = False
        success = False
        
        # 1. 입력 타입에 따른 데이터 준비 (GeoJSON화)
        if input_type == 'dxf':
            if download_from_r2(f"cad_data/CAD_{project_id}.dxf", "input.dxf"):
                if dxf_to_geojson(project_id, source_crs, layers, centerline_layer, reverse_chainage):
                    conversion_ready = True
        
        elif input_type == 'shp':
            # Shapefile은 .shp, .shx, .dbf 필수 다운로드
            download_ok = True
            for ext in ['.shp', '.shx', '.dbf', '.prj']:
                if not download_from_r2(f"cad_data/CAD_{project_id}{ext}", f"input{ext}"):
                    if ext != '.prj': download_ok = False # prj는 없어도 시도 가능
            
            if download_ok and convert_spatial_to_geojson("input.shp", source_crs):
                conversion_ready = True
        
        elif input_type == 'zip':
            # 압축파일 형태의 Shapefile
            if download_from_r2(f"cad_data/CAD_{project_id}.zip", "input.zip"):
                with zipfile.ZipFile("input.zip", 'r') as zip_ref:
                    zip_ref.extractall("temp_shp")
                
                # 압축 해제된 파일 중 .shp 찾기
                target_shp = None
                for root, dirs, files in os.walk("temp_shp"):
                    for file in files:
                        if file.lower().endswith(".shp"):
                            target_shp = os.path.join(root, file)
                            break
                
                if target_shp and convert_spatial_to_geojson(target_shp, source_crs):
                    conversion_ready = True
        
        # 2. PMTiles 변환 및 업로드
        if conversion_ready:
            if convert_to_pmtiles():
                if upload_to_r2(project_id, cache_control, source_crs):
                    # DXF 타입인 경우에만 도면 연장/수량 재계산 수행
                    if input_type == 'dxf':
                        run_recalculation(project_id, "input.dxf")
                    success = True

        if success:
            supabase = get_supabase_client()
            if supabase:
                supabase.table("cad_projects").update({"status": "COMPLETED"}).eq("id", project_id).execute()
            print("All steps completed successfully.")
        else:
            print("Conversion process failed. Updating project status to FAILED...")
            supabase = get_supabase_client()
            if supabase and project_id:
                supabase.table("cad_projects").update({"status": "FAILED"}).eq("id", project_id).execute()
            sys.exit(1)
            
    except json.JSONDecodeError:
        print("Invalid JSON payload")
        sys.exit(1)
