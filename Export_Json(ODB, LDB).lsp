;;; ==========================================================================
;;; Command: ODB / LDB
;;; Description: Exports selected object data to JSON for ASIN Tech App
;;; ODB: Manual selection (Object Database)
;;; LDB: Layer selection via dialog (Layer Database)
;;; Compatibility: AutoCAD Map 3D 2000+
;;; Output: C:\Temp\asin_autocad_data(n).json
;;; ==========================================================================

;;; Common Export Function
(defun asin-export-json (ss / i ent ent_data layer handle type wkt vertices p1 p2 ins_pt text_content block_name rotation json_str base_path ext file_path counter file_handle first_v_str v_str v_count is_closed)
  (if ss
    (progn
      (setq json_str "[")
      (setq i 0)
      
      ;; Iterate through selection
      (repeat (sslength ss)
        (setq ent (ssname ss i))
        (setq ent_data (entget ent))
        
        ;; Extract Properties
        (setq type (cdr (assoc 0 ent_data)))
        (setq layer (cdr (assoc 8 ent_data)))
        (setq handle (cdr (assoc 5 ent_data)))
        (setq ins_pt (cdr (assoc 10 ent_data))) ; Insertion point (DXF 10)
        
        ;; Extract Text and Rotation (if MTEXT or TEXT)
        (setq text_content "")
        (setq block_name "")
        (setq rotation 0.0)
        
        (if (or (= type "TEXT") 
                (= type "MTEXT"))
          (progn
            (setq text_content (cdr (assoc 1 ent_data)))
            (if (assoc 50 ent_data)
              ; Convert radians (DXF 50) to degrees for easier use in QGIS
              (setq rotation (* (/ (cdr (assoc 50 ent_data)) pi) 180.0))
            )
          )
        )
        
        ;; Extract Block Name (if INSERT)
        (if (= type "INSERT")
          (progn
            (setq block_name (cdr (assoc 2 ent_data)))
            (if (assoc 50 ent_data)
              (setq rotation (* (/ (cdr (assoc 50 ent_data)) pi) 180.0))
            )
          )
        )

        ;; Format Coordinates
        (if (null ins_pt) (setq ins_pt '(0.0 0.0 0.0)))
        
        ;; Generate WKT (Well-Known Text) Geometry
        (setq wkt "")
        (cond
          ((= type "LINE")
            (setq p1 (cdr (assoc 10 ent_data)))
            (setq p2 (cdr (assoc 11 ent_data)))
            (setq wkt (strcat "LINESTRING(" (rtos (car p1) 2 6) " " (rtos (cadr p1) 2 6) "," (rtos (car p2) 2 6) " " (rtos (cadr p2) 2 6) ")"))
          )
          ((= type "LWPOLYLINE")
            (setq vertices "")
            (setq first_v_str nil)
            (setq v_count 0)
            (foreach item ent_data
              (if (= (car item) 10)
                (progn
                  (setq v_count (1+ v_count))
                  (setq v_str (strcat (rtos (cadr item) 2 6) " " (rtos (caddr item) 2 6)))
                  (if (null first_v_str) (setq first_v_str v_str))
                  (setq vertices (strcat vertices (if (> (strlen vertices) 0) "," "") v_str))
                )
              )
            )
            ;; Check if closed (DXF 70 bit 1)
            (setq is_closed (= (logand (cdr (assoc 70 ent_data)) 1) 1))
            (cond
              ((and is_closed (> v_count 2)) ; 닫힌 폴리선도 LINESTRING으로 변환 (폴리곤 제외)
                (setq wkt (strcat "LINESTRING(" vertices "," first_v_str ")")))
              ((and is_closed (= v_count 2)) ; 점이 2개인데 닫혀있으면 선으로 처리
                (setq wkt (strcat "LINESTRING(" vertices "," first_v_str ")")))
              ((> v_count 1) ; 점이 2개 이상인 열린 선
                (setq wkt (strcat "LINESTRING(" vertices ")")))
              (T ; 점이 1개 이하면 포인트로 처리
                (setq wkt (strcat "POINT(" (if first_v_str first_v_str "0 0") ")")))
            )
          )
          (T ; Default to POINT (TEXT, INSERT, POINT, etc.)
            (setq wkt (strcat "POINT(" (rtos (car ins_pt) 2 6) " " (rtos (cadr ins_pt) 2 6) ")"))
          )
        )

        ;; Construct JSON Object String
        (if (> i 0) (setq json_str (strcat json_str ",")))
        (setq json_str (strcat json_str 
          "{"
          "\"handle\":\"" handle "\","
          "\"type\":\"" type "\","
          "\"layer\":\"" layer "\","
          "\"block_name\":\"" block_name "\","
          "\"text\":\"" text_content "\","
          "\"x\":" (rtos (car ins_pt) 2 6) ","
          "\"y\":" (rtos (cadr ins_pt) 2 6) ","
          "\"rotation\":" (rtos rotation 2 8) ","
          "\"wkt\":\"" wkt "\""
          "}"
        ))
        
        (setq i (1+ i))
      )
      (setq json_str (strcat json_str "]"))

      ;; Write to File
      (setq base_path "C:\\Temp\\asin_autocad_data")
      (setq ext ".json")
      (setq file_path (strcat base_path ext))
      (setq counter 1)
      
      (while (findfile file_path)
        (setq file_path (strcat base_path "(" (itoa counter) ")" ext))
        (setq counter (1+ counter))
      )
      
      (setq file_handle (open file_path "w"))
      
      (if file_handle
        (progn
          (write-line json_str file_handle)
          (close file_handle)
          (princ (strcat "\n[Success] Data exported to " file_path))
          (princ (strcat "\n[Info] " (itoa (sslength ss)) " objects selected."))
        )
        (princ "\n[Error] Could not write file. Make sure C:\\Temp folder exists.")
      )
    )
    (princ "\n[Info] No objects selected.")
  )
)

(defun c:ODB (/ ss)
  (vl-load-com) ; Load Visual LISP extensions

  ;; 1. Select Objects
  (prompt "\nSelect objects to export to Supabase: ")
  (setq ss (ssget))
  (asin-export-json ss)
  (princ)
)

(defun c:LDB (/ layers selected-layers filter ss get-all-layers select-layers-dialog)
  (vl-load-com)

  ;; Helper: Get all layers
  (defun get-all-layers (/ lay lst)
    (while (setq lay (tblnext "LAYER" (not lay)))
      (setq lst (cons (cdr (assoc 2 lay)) lst))
    )
    (acad_strlsort lst)
  )

  ;; Helper: Show DCL dialog
  (defun select-layers-dialog (layer-list / dcl_id fname f selected-indices result)
    (setq fname (vl-filename-mktemp "layers.dcl"))
    (setq f (open fname "w"))
    (write-line "layer_sel : dialog { label = \"Select Layers to Export\";" f)
    (write-line " : list_box { key = \"laylist\"; multiple_select = true; width = 40; height = 20; }" f)
    (write-line " ok_cancel; }" f)
    (close f)
    
    (setq dcl_id (load_dialog fname))
    (if (not (new_dialog "layer_sel" dcl_id)) (exit))
    
    (start_list "laylist")
    (mapcar 'add_list layer-list)
    (end_list)
    
    (action_tile "laylist" "(setq selected-indices $value)")
    
    (if (= (start_dialog) 1)
      (if selected-indices
        (setq result (mapcar '(lambda (n) (nth n layer-list)) (read (strcat "(" selected-indices ")"))))
      )
    )
    (unload_dialog dcl_id)
    (vl-file-delete fname)
    result
  )

  ;; Main LDB Logic
  (setq layers (get-all-layers))
  (setq selected-layers (select-layers-dialog layers))
  
  (if selected-layers
    (progn
      ;; Construct filter string
      (setq filter "")
      (foreach lay selected-layers
        (setq filter (strcat filter lay ","))
      )
      (setq filter (substr filter 1 (1- (strlen filter))))
      
      (prompt (strcat "\nExporting objects on layers: " filter))
      (setq ss (ssget "X" (list (cons 8 filter))))
      (asin-export-json ss)
    )
    (princ "\nCancelled.")
  )
  (princ)
)
