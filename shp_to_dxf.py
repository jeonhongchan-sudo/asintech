import shapefile # pyshp
import ezdxf
import os
import sys
import zipfile
import tempfile
import shutil
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

def convert_shp_to_dxf(shp_path, output_dxf, layer_field='LAYER'):
    """
    SHP 파일을 읽어 지정된 필드값을 레이어 이름으로 매핑하여 DXF로 변환합니다.
    """
    if not os.path.exists(shp_path):
        print(f"오류: 파일을 찾을 수 없습니다 -> {shp_path}")
        return

    print(f"데이터 분석 중: {shp_path}")
    
    # 1. SHP 파일 읽기
    try:
        # 한글 속성 깨짐 방지를 위해 cp949 인코딩 사용
        sf = shapefile.Reader(shp_path, encoding='cp949')
    except Exception as e:
        print(f"SHP 로드 실패: {e}")
        return

    # 2. 필드 인덱스 찾기
    # sf.fields[0]은 삭제 플래그이므로 실제 데이터 필드는 1번부터 시작합니다.
    fields = [f[0].upper() for f in sf.fields[1:]]
    try:
        layer_idx = fields.index(layer_field.upper())
    except ValueError:
        print(f"경고: '{layer_field}' 필드를 찾을 수 없습니다. 필드 목록: {fields}")
        print("기본 레이어 '0'을 사용합니다.")
        layer_idx = -1

    # 3. DXF 문서 생성 (AutoCAD Map 3D 2000 호환을 위해 R2000 설정)
    doc = ezdxf.new('R2000')
    msp = doc.modelspace()

    print(f"변환 시작 (총 객체 수: {len(sf)})")

    for shape_rec in sf.shapeRecords():
        shape = shape_rec.shape
        record = shape_rec.record
        
        # 레이어 이름 결정
        layer_name = "0"
        if layer_idx != -1:
            val = str(record[layer_idx]).strip()
            if val:
                # DXF 레이어 이름에 사용할 수 없는 특수문자 정제
                layer_name = val.replace(" ", "_").replace("/", "_")
        
        # 레이어가 없으면 생성
        if layer_name not in doc.layers:
            doc.layers.new(name=layer_name)

        # 4. 기하 타입별 변환
        # Point
        if shape.shapeType == shapefile.POINT:
            msp.add_point(shape.points[0], dxfattribs={'layer': layer_name})
        
        # LineString (Polyline)
        elif shape.shapeType in [shapefile.POLYLINE, shapefile.POLYLINEZ]:
            # parts는 폴리라인이 끊겨있는 구간(Ring)의 시작 인덱스
            parts = list(shape.parts) + [len(shape.points)]
            for i in range(len(parts)-1):
                pts = shape.points[parts[i]:parts[i+1]]
                if len(pts) >= 2:
                    msp.add_lwpolyline(pts, dxfattribs={'layer': layer_name})
        
        # Polygon
        elif shape.shapeType in [shapefile.POLYGON, shapefile.POLYGONZ]:
            parts = list(shape.parts) + [len(shape.points)]
            for i in range(len(parts)-1):
                pts = shape.points[parts[i]:parts[i+1]]
                if len(pts) >= 3:
                    # 폴리곤은 닫힌 폴리라인으로 생성
                    msp.add_lwpolyline(pts, is_closed=True, dxfattribs={'layer': layer_name})

    doc.saveas(output_dxf)
    return True

class ShpConverterUI:
    def __init__(self, root):
        self.root = root
        self.root.title("ASIN SHP to DXF Converter")
        self.root.geometry("500x250")
        
        self.zip_path = tk.StringVar()
        self.layer_field = tk.StringVar(value="LAYER")
        
        self.create_widgets()

    def create_widgets(self):
        frame = ttk.Frame(self.root, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)

        # ZIP 파일 선택
        ttk.Label(frame, text="SHP 압축파일 (ZIP):").grid(row=0, column=0, sticky=tk.W, pady=5)
        ttk.Entry(frame, textvariable=self.zip_path, width=40).grid(row=1, column=0, padx=(0, 5))
        ttk.Button(frame, text="찾아보기", command=self.browse_zip).grid(row=1, column=1)

        # 레이어 필드 설정
        ttk.Label(frame, text="레이어명으로 사용할 필드 (기본: LAYER):").grid(row=2, column=0, sticky=tk.W, pady=(15, 5))
        ttk.Entry(frame, textvariable=self.layer_field, width=20).grid(row=3, column=0, sticky=tk.W)

        # 실행 버튼
        ttk.Button(frame, text="DXF 변환 및 다운로드 저장", command=self.run_conversion).grid(row=4, column=0, columnspan=2, pady=20)

    def browse_zip(self):
        path = filedialog.askopenfilename(filetypes=[("ZIP Files", "*.zip")])
        if path:
            self.zip_path.set(path)

    def run_conversion(self):
        zip_file = self.zip_path.get()
        field = self.layer_field.get()

        if not zip_file or not os.path.exists(zip_file):
            messagebox.showerror("오류", "유효한 ZIP 파일을 선택해주세요.")
            return

        # 다운로드 폴더 경로 설정
        download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        output_name = os.path.splitext(os.path.basename(zip_file))[0] + ".dxf"
        output_path = os.path.join(download_dir, output_name)

        # 임시 디렉토리 생성
        temp_dir = tempfile.mkdtemp()
        
        try:
            # ZIP 압축 해제
            with zipfile.ZipFile(zip_file, 'r') as z:
                z.extractall(temp_dir)
            
            # .shp 파일 찾기
            shp_files = []
            for root, dirs, files in os.walk(temp_dir):
                for f in files:
                    if f.lower().endswith(".shp"):
                        shp_files.append(os.path.join(root, f))
            
            if not shp_files:
                raise Exception("압축파일 내에 .shp 파일이 없습니다.")

            # 첫 번째 발견된 SHP 파일 변환
            # (여러 SHP가 있을 경우 루프를 돌려 하나로 합치거나 각각 생성하도록 확장 가능)
            success = convert_shp_to_dxf(shp_files[0], output_path, field)
            
            if success:
                messagebox.showinfo("성공", f"변환이 완료되었습니다!\n\n저장위치: {output_path}")
                # 폴더 열기 (윈도우 전용)
                os.startfile(download_dir)

        except Exception as e:
            messagebox.showerror("실패", f"변환 중 오류 발생:\n{str(e)}")
        finally:
            # 임시 파일 삭제
            shutil.rmtree(temp_dir)

if __name__ == "__main__":
    root = tk.Tk()
    # 앱 아이콘이나 스타일을 asin_app.py와 유사하게 맞출 수 있습니다.
    app = ShpConverterUI(root)
    root.mainloop()
