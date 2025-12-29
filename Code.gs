// ==========================================
// 설정 상수
// ==========================================
const CONFIG = {
  PARENT_FOLDER_ID: '1q3P5mxJ1Ze578xNzOdmOOAeMpS6xQDoh',
  GUIDELINE_FILE_ID: '1mysxDT9bfxcdh2-DXDW9NLnOZOCRQ7lF',
  NUMERIC_MAP_FILE_ID: '1-TI3CJ1kR5uTOi1Zjs1EWcreqWsAlR9v',
  SVG_FOLDER_ID: '1t2wmBiLEn9PZ41AT9ojR1uNrQSh-r4Ou', // SVG 폴더 ID
  CACHE_KEY_PROJECTS: 'CACHE_PROJECTS_LIST',
  CACHE_DURATION: 21600
};

// ==========================================
// 웹앱 진입점
// ==========================================
function doGet(e) {
  if (e && e.parameter && e.parameter.action) {
    return handleApiRequest(e);
  }
  // HTML 파일 삭제 후 API 서버로만 작동함을 알림
  return ContentService.createTextOutput("Asin Tech API Server is running. (v2.3)");
}

function doPost(e) {
  const params = e.postData ? JSON.parse(e.postData.contents) : {};
  if (!e.postData && e.parameter) Object.assign(params, e.parameter);
  return handleApiRequest({ parameter: params });
}

function handleApiRequest(e) {
  const action = e.parameter.action;
  let response = {};
  
  try {
    switch (action) {
      case 'getProjects': response = getProjects(); break;
      case 'getAllProjects': response = getProjects(); break; // QGIS/External Alias
      case 'createProject': response = createProject(e.parameter.projectName, e.parameter.skipDb); break;
      case 'renameProject': response = renameProject(e.parameter.projectId, e.parameter.newName); break;
      case 'deleteProject': response = deleteProject(e.parameter.projectId); break;
      case 'getPhotosByProject': response = getPhotos(e.parameter.projectId); break;
      case 'getFiles': response = getPhotos(e.parameter.projectId); break; // QGIS/External Alias
      case 'uploadPhoto': response = uploadPhoto(e.parameter.projectId, e.parameter.fileData, e.parameter.fileName, e.parameter.skipDb); break;
      case 'deletePhoto': response = deletePhoto(e.parameter.fileId); break;
      case 'renamePhoto': response = renamePhoto(e.parameter.fileId, e.parameter.newFileName); break;
      case 'synchronizePhotos': response = synchronizePhotos(e.parameter.projectId, e.parameter.continuationToken); break;
      case 'synchronizeAll': response = synchronizeAllProjects(); break;
      case 'checkForUpdates': response = checkForUpdates(e.parameter.lastSyncTime); break;
      case 'exportToCSV': response = exportToCSV(e.parameter.projectId); break;
      
      // [CAD 관리] 프로젝트 및 데이터 관리
      case 'getCadProjects': response = getCadProjects(); break;
      case 'createCadProject': response = createCadProject(e.parameter.name); break;
      case 'renameCadProject': response = renameCadProject(e.parameter.id, e.parameter.newName); break;
      case 'deleteCadProject': response = deleteCadProject(e.parameter.id); break;
      case 'getCadObjects': response = getCadObjects(e.parameter.projectId); break;
      case 'uploadCadObjects': response = uploadCadObjects(e.parameter.projectId, e.parameter.data); break;
      case 'deleteCadObjects': response = deleteCadObjects(e.parameter.ids); break;
      case 'deleteProjectCadObjects': response = deleteProjectCadObjects(e.parameter.projectId); break;

      // 지침서 PDF
      case 'getGuidelineMeta': response = getGuidelineMeta(); break;
      case 'getGuidelinePdf': response = getGuidelinePdf(); break;
      case 'getNumericMapPdf': response = getNumericMapPdf(); break;

      // [SVG 관리] 구글 드라이브 SVG 폴더 동기화
      case 'getSvgs': response = getSvgs(e.parameter.folderId); break;
      case 'uploadSvg': response = uploadSvg(e.parameter.folderId, e.parameter.fileData, e.parameter.fileName, e.parameter.skipDb); break;
      case 'synchronizeSvgs': response = syncSvgLibrary(e.parameter.continuationToken); break;

      // [설정] Supabase 설정 반환 (Python 앱 직접 통신용)
      case 'getSupabaseConfig': response = getSupabaseConfig(); break;

      // [GitHub Action] 변환 트리거
      case 'triggerGitHubAction': response = triggerGitHubAction(e.parameter.projectId); break;

      default: response = { success: false, error: 'Unknown action' };
    }
  } catch (error) {
    response = { success: false, error: error.toString() };
  }
  return ContentService.createTextOutput(JSON.stringify(response)).setMimeType(ContentService.MimeType.JSON);
}

// ==========================================
// 유틸리티 및 프로젝트 관리 함수
// ==========================================
function formatDate(date) {
  if (!date) return '';
  if (date instanceof Date) return date.toISOString();
  return String(date);
}

function checkForUpdates(clientLastSyncTime) {
  // [Supabase] DB 기반이므로 시트 변경 감지 로직 제거
  return { success: true, hasChanges: false };
}

function getGuidelineMeta() {
  try {
    const file = DriveApp.getFileById(CONFIG.GUIDELINE_FILE_ID);
    return { success: true, lastUpdated: file.getLastUpdated().getTime() };
  } catch (e) { return { success: false, error: e.toString() }; }
}

function getGuidelinePdf() {
  try {
    const file = DriveApp.getFileById(CONFIG.GUIDELINE_FILE_ID);
    const blob = file.getBlob();
    return { success: true, data: Utilities.base64Encode(blob.getBytes()), name: file.getName(), lastUpdated: file.getLastUpdated().getTime() };
  } catch (e) { return { success: false, error: '지침서 로드 실패: ' + e.toString() }; }
}

function getNumericMapPdf() {
  try {
    const file = DriveApp.getFileById(CONFIG.NUMERIC_MAP_FILE_ID);
    const blob = file.getBlob();
    return { success: true, data: Utilities.base64Encode(blob.getBytes()), name: file.getName(), lastUpdated: file.getLastUpdated().getTime() };
  } catch (e) { return { success: false, error: '수치지도 도엽번호 로드 실패: ' + e.toString() }; }
}

function clearProjectCache() { try { CacheService.getScriptCache().remove(CONFIG.CACHE_KEY_PROJECTS); } catch (e) {} }

function getProjects() {
  try {
    const cache = CacheService.getScriptCache();
    const cachedData = cache.get(CONFIG.CACHE_KEY_PROJECTS);
    if (cachedData) return JSON.parse(cachedData);

    // [Supabase] 프로젝트 목록 조회 (생성일 역순)
    const data = callSupabase('projects?select=*&order=created_at.desc', 'get');
    
    if (!Array.isArray(data)) {
      return { success: true, projects: [] };
    }

    // DB 컬럼(snake_case)을 앱에서 사용하는 이름(camelCase)으로 변환
    const projects = data.map(row => ({ name: row.project_name, id: row.folder_id, createdDate: row.created_at, status: row.status }));
    
    const result = { success: true, projects: projects };
    try { cache.put(CONFIG.CACHE_KEY_PROJECTS, JSON.stringify(result), CONFIG.CACHE_DURATION); } catch(e) {}
    return result;
  } catch (error) { return { success: false, error: '프로젝트 조회 오류: ' + error.toString() }; }
}

function synchronizeAllProjects() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) return { success: false, error: '다른 작업이 진행 중입니다. 잠시 후 다시 시도해주세요.' };
  
  try {
    clearProjectCache();
    
    // [SVG 동기화] 전체 동기화 시 SVG 라이브러리도 함께 동기화
    const svgResult = syncSvgLibrary();
    let svgMsg = "";
    if (svgResult.success) {
      svgMsg = ` [SVG: ${svgResult.message}]`;
    } else {
      svgMsg = ` [SVG 오류: ${svgResult.error}]`;
    }
    
    // 1. 구글 드라이브 폴더 목록 가져오기
    let parentFolder = DriveApp.getFolderById(CONFIG.PARENT_FOLDER_ID);
    const driveFoldersMap = new Map();
    const folders = parentFolder.getFolders();
    while (folders.hasNext()) { const folder = folders.next(); driveFoldersMap.set(folder.getId(), { name: folder.getName(), createdDate: folder.getDateCreated() }); }

    // 2. Supabase DB 프로젝트 목록 가져오기
    const dbProjects = callSupabase('projects?select=*', 'get');
    const dbMap = new Map();
    if (Array.isArray(dbProjects)) {
      dbProjects.forEach(p => dbMap.set(p.folder_id, p));
    }

    let addedCount = 0, updatedCount = 0, deletedCount = 0;

    // 3. 드라이브 -> DB 동기화 (추가 및 이름 변경)
    for (const [id, info] of driveFoldersMap) {
      if (dbMap.has(id)) {
        // 이미 DB에 있으면 이름 변경 확인
        if (dbMap.get(id).project_name !== info.name) {
          callSupabase(`projects?folder_id=eq.${id}`, 'patch', { project_name: info.name });
          updatedCount++;
        }
        dbMap.delete(id); // 처리된 항목 제거
      } else {
        // DB에 없으면 추가
        callSupabase('projects', 'post', {
          project_name: info.name,
          folder_id: id,
          status: 'Active',
          created_at: info.createdDate.toISOString()
        });
        addedCount++;
      }
    }

    // 4. DB -> 드라이브 동기화 (드라이브에 없는 프로젝트 DB에서 삭제)
    for (const folderId of dbMap.keys()) {
      callSupabase(`projects?folder_id=eq.${folderId}`, 'delete');
      deletedCount++;
    }

    return { success: true, message: `동기화 완료: +${addedCount} / ~${updatedCount} / -${deletedCount}${svgMsg}`, syncTime: new Date().getTime() };
  } catch (error) { return { success: false, error: '전체 동기화 오류: ' + error.toString() }; }
  finally { lock.releaseLock(); }
}

function createProject(projectName, skipDb) {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(10000)) return { success: false, error: '서버가 바쁩니다. 잠시 후 다시 시도해주세요.' };
  
  // skipDb가 'true' 문자열일 경우에만 DB 저장을 건너뜀
  const shouldSkipDb = String(skipDb).toLowerCase() === 'true';

  try {
    clearProjectCache();
    
    // [Supabase] 중복 이름 확인 (DB 모드일 때만 수행)
    if (!shouldSkipDb) {
      const existing = callSupabase(`projects?project_name=eq.${encodeURIComponent(projectName)}`, 'get');
      if (Array.isArray(existing) && existing.length > 0) {
        return { success: false, error: '이미 존재하는 프로젝트명' };
      }
    }
    
    const projectsFolder = DriveApp.getFolderById(CONFIG.PARENT_FOLDER_ID);
    const newFolder = projectsFolder.createFolder(projectName);
    newFolder.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
    
    // [Supabase] 프로젝트 추가 (skipDb가 아닐 때만 수행)
    if (!shouldSkipDb) {
      callSupabase('projects', 'post', {
        project_name: projectName,
        folder_id: newFolder.getId(),
        status: 'Active'
      });
    }

    // 로컬 DB 저장을 위해 folderId 반환
    return { success: true, message: '프로젝트 생성 성공', folderId: newFolder.getId() };
  } catch (error) { return { success: false, error: '생성 오류: ' + error.toString() }; }
  finally { lock.releaseLock(); }
}

function deleteProject(projectId) {
  try {
    clearProjectCache();
    
    // [Supabase] 프로젝트 삭제 (Cascade 설정으로 사진 데이터도 자동 삭제됨)
    callSupabase(`projects?folder_id=eq.${projectId}`, 'delete');

    try { DriveApp.getFolderById(projectId).setTrashed(true); } catch (e) {}
    return { success: true, message: '삭제 완료' };
  } catch (error) { return { success: false, error: '삭제 오류: ' + error.toString() }; }
}

function renameProject(projectId, newName) {
  try {
    clearProjectCache();
    
    // [Supabase] 이름 변경
    callSupabase(`projects?folder_id=eq.${projectId}`, 'patch', { project_name: newName });

    try { DriveApp.getFolderById(projectId).setName(newName); } catch(e) {}
    return { success: true, message: '이름 변경 완료' };
  } catch (error) { return { success: false, error: '변경 오류: ' + error.toString() }; }
}

function uploadPhoto(projectId, fileData, fileName, skipDb) {
  try {
    const shouldSkipDb = String(skipDb).toLowerCase() === 'true';

    // 1. 구글 드라이브에 파일 업로드
    const contentType = fileData.substring(5, fileData.indexOf(';'));
    const bytes = Utilities.base64Decode(fileData.substring(fileData.indexOf(',') + 1));
    const blob = Utilities.newBlob(bytes, contentType, fileName);
    const file = DriveApp.getFolderById(projectId).createFile(blob);
    file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
    
    // 2. [Supabase] 사진 정보 등록
    const fileId = file.getId();
    const fileUrl = `https://drive.google.com/file/d/${fileId}/view?usp=sharing`;
    const now = new Date();

    // [Supabase] 사진 정보 등록 (skipDb가 아닐 때만 수행)
    if (!shouldSkipDb) {
      callSupabase('photos', 'post', {
        project_folder_id: projectId,
        file_id: fileId,
        file_name: fileName,
        file_url: fileUrl,
        created_at: now.toISOString()
      });
    }

    return { success: true, message: '업로드 성공', photo: { fileId: fileId, fileName: fileName, url: fileUrl, uploadDate: formatDate(now) } };
  } catch (error) { return { success: false, error: '업로드 오류: ' + error.toString() }; }
}

function getPhotos(projectId) {
  try {
    // [Supabase] 사진 목록 조회 (최신순 정렬)
    const data = callSupabase(`photos?project_folder_id=eq.${projectId}&select=*&order=created_at.desc`, 'get');
    
    if (!Array.isArray(data)) return { success: true, photos: [] };

    const photos = data.map(row => ({
      fileName: row.file_name,
      url: row.file_url,
      fileId: row.file_id,
      uploadDate: row.created_at
    }));
    
    return { success: true, photos: photos };
  } catch (error) { return { success: false, error: '조회 오류: ' + error.toString() }; }
}

function deletePhoto(fileId) {
  try {
    // 1. 드라이브 파일 삭제
    try { DriveApp.getFileById(fileId).setTrashed(true); } catch(e) {}
    
    // 2. [Supabase] 데이터 삭제
    callSupabase(`photos?file_id=eq.${fileId}`, 'delete');
    
    return { success: true, message: '삭제됨' };
  } catch (error) { return { success: false, error: '삭제 오류: ' + error.toString() }; }
}

function renamePhoto(fileId, newFileName) {
  try {
    // 1. 드라이브 파일 이름 변경
    try { DriveApp.getFileById(fileId).setName(newFileName); } catch(e) {}
    
    // 2. [Supabase] 데이터 수정
    callSupabase(`photos?file_id=eq.${fileId}`, 'patch', { file_name: newFileName });
    
    return { success: true, message: '이름 변경됨' };
  } catch (error) { return { success: false, error: '변경 오류: ' + error.toString() }; }
}

function synchronizePhotos(projectId, continuationToken) {
  try {
    const startTime = new Date().getTime();
    const TIME_LIMIT = 210 * 1000;
    
    // 1. Supabase DB에서 현재 프로젝트의 사진 목록 가져오기
    const dbPhotos = callSupabase(`photos?project_folder_id=eq.${projectId}&select=file_id,file_name`, 'get');
    const dbMap = new Map();
    if (Array.isArray(dbPhotos)) {
      dbPhotos.forEach(p => dbMap.set(p.file_id, p.file_name));
    }

    // 2. 드라이브 파일 순회
    let files = continuationToken ? DriveApp.continueFileIterator(continuationToken) : DriveApp.getFolderById(projectId).getFiles();
    let processedCount = 0;
    let addedCount = 0;
    
    while (files.hasNext()) {
      if (new Date().getTime() - startTime > TIME_LIMIT) {
          return { success: true, finished: false, continuationToken: files.getContinuationToken(), processedCount: processedCount, message: `진행 중...` };
      }
      const file = files.next();
      processedCount++;
      if (!file.getMimeType().startsWith('image/')) continue;
      
      const fileId = file.getId();
      const fileName = file.getName();

      if (dbMap.has(fileId)) {
        // 이름 변경 확인
        if (dbMap.get(fileId) !== fileName) {
          callSupabase(`photos?file_id=eq.${fileId}`, 'patch', { file_name: fileName });
        }
      } else {
        // DB에 없으면 추가
        callSupabase('photos', 'post', {
          project_folder_id: projectId,
          file_id: fileId,
          file_name: fileName,
          file_url: `https://drive.google.com/file/d/${fileId}/view?usp=sharing`,
          created_at: file.getDateCreated().toISOString()
        });
        addedCount++;
      }
    }
    
    return { success: true, finished: true, processedCount: processedCount, message: `동기화 완료! (${addedCount}개 추가됨)` };
  } catch (error) { return { success: false, error: '동기화 오류: ' + error.toString() }; }
}

function exportToCSV(projectId) {
  try {
    // [Supabase] CSV 생성을 위한 데이터 조회
    const data = callSupabase(`photos?project_folder_id=eq.${projectId}&select=file_name,file_url`, 'get');
    
    let csv = '\uFEFFphoto_filename,photo_url\n';
    if (Array.isArray(data)) {
      data.forEach(row => {
        csv += `"${row.file_name}","${row.file_url}"\n`;
      });
    }
    return { success: true, csv: csv, fileName: `photos.csv` };
  } catch (error) { return { success: false, error: 'CSV 오류: ' + error.toString() }; }
}

// [CAD 관리] 프로젝트 관련 함수
function getCadProjects() {
  const data = callSupabase('cad_projects?select=*&order=created_at.desc', 'get');
  if (!Array.isArray(data)) return { success: true, projects: [] };
  return { success: true, projects: data };
}

function createCadProject(name) {
  try {
    // [수정] 생성된 프로젝트 정보를 반환받아 ID 전달
    const res = callSupabase('cad_projects', 'post', { name: name });
    if (Array.isArray(res) && res.length > 0) {
      return { success: true, message: 'CAD 프로젝트 생성 완료', id: res[0].id };
    }
    return { success: true, message: 'CAD 프로젝트 생성 완료 (ID 확인 불가)' };
  } catch (e) { return { success: false, error: e.toString() }; }
}

function renameCadProject(id, newName) {
  try {
    callSupabase(`cad_projects?id=eq.${id}`, 'patch', { name: newName });
    return { success: true, message: '이름 변경 완료' };
  } catch (e) { return { success: false, error: e.toString() }; }
}

function deleteCadProject(id) {
  try {
    // Cascade 설정이 되어 있다면 프로젝트 삭제 시 하위 객체도 자동 삭제됨
    callSupabase(`cad_projects?id=eq.${id}`, 'delete');
    return { success: true, message: '프로젝트 삭제 완료' };
  } catch (e) { return { success: false, error: e.toString() }; }
}

function getCadObjects(projectId) {
  try {
    // 해당 프로젝트의 객체 조회 (ID순 정렬)
    const data = callSupabase(`cad_objects?project_id=eq.${projectId}&select=*&order=id.asc`, 'get');
    if (!Array.isArray(data)) return { success: true, objects: [] };
    return { success: true, objects: data };
  } catch (e) { return { success: false, error: e.toString() }; }
}

// [CAD 관리] 데이터 업로드 (프로젝트 ID 포함)
function uploadCadObjects(projectId, data) {
  try {
    const objects = typeof data === 'string' ? JSON.parse(data) : data;
    if (!Array.isArray(objects) || objects.length === 0) return { success: false, error: '데이터가 없습니다.' };

    // Supabase에 전송할 페이로드 구성
    // LISP에서 생성한 WKT를 그대로 사용 (없으면 POINT 생성)
    const payload = objects.map(obj => {
      let geomStr = obj.wkt;
      let x = obj.x ? parseFloat(obj.x) : null;
      let y = obj.y ? parseFloat(obj.y) : null;

      // WKT가 없고 x,y가 있으면 POINT 생성 (기존 로직 호환)
      if (!geomStr && x !== null && y !== null) {
        geomStr = `POINT(${x} ${y})`;
      }

      // [Fix] SRID 4326 명찰 붙이기 (PostGIS 에러 방지)
      if (geomStr && !geomStr.startsWith('SRID=')) {
        geomStr = `SRID=4326;${geomStr}`;
      }

      return {
        project_id: projectId,
        handle: obj.handle,
        layer: obj.layer,
        block_name: obj.block_name,
        text_content: obj.text,
        x_coord: x,
        y_coord: y,
        rotation: obj.rotation ? parseFloat(obj.rotation) : null,
        geom: geomStr,
        created_at: new Date().toISOString()
      };
    });

    // Supabase 'cad_objects' 테이블에 upsert (handle 기준 중복 시 업데이트)
    // Prefer: resolution=merge-duplicates 헤더가 있어야 handle 충돌 시 에러가 안 나고 업데이트됨
    const headers = { 'Prefer': 'resolution=merge-duplicates' };
    const result = callSupabase('cad_objects', 'post', payload, headers);
    
    return { success: true, message: `${objects.length}개 객체 저장 완료` };
  } catch (error) {
    return { success: false, error: 'CAD 업로드 오류: ' + error.toString() };
  }
}

// [CAD 관리] 데이터 삭제 (다중 선택 지원)
function deleteCadObjects(ids) {
  try {
    // ids 예시: "1,2,3" (문자열)
    callSupabase(`cad_objects?id=in.(${ids})`, 'delete');
    return { success: true, message: '선택한 객체가 삭제되었습니다.' };
  } catch (e) { return { success: false, error: e.toString() }; }
}

// [CAD 관리] 프로젝트 내 모든 객체 삭제 (전체 덮어쓰기용)
function deleteProjectCadObjects(projectId) {
  try {
    callSupabase(`cad_objects?project_id=eq.${projectId}`, 'delete');
    return { success: true, message: '프로젝트 데이터 초기화 완료' };
  } catch (e) { return { success: false, error: e.toString() }; }
}

// [SVG 관리] SVG 목록 조회 (중복 확인용)
function getSvgs(folderId) {
  try {
    const data = callSupabase('svg_library?select=block_name,file_id', 'get');
    if (!Array.isArray(data)) return { success: true, svgs: [] };
    
    // Python 클라이언트가 fileName을 기준으로 비교하므로 포맷 맞춤
    const svgs = data.map(row => ({
      fileName: row.block_name + '.svg',
      fileId: row.file_id
    }));
    return { success: true, svgs: svgs };
  } catch (e) { return { success: false, error: 'SVG 목록 조회 실패: ' + e.toString() }; }
}

// [SVG 관리] SVG 파일 업로드 및 DB 등록
function uploadSvg(folderId, fileData, fileName, skipDb) {
  try {
    const shouldSkipDb = String(skipDb).toLowerCase() === 'true';

    // 1. 구글 드라이브 업로드
    const contentType = 'image/svg+xml';
    const base64Data = fileData.split(',')[1]; // data:image/svg+xml;base64, 제거
    const bytes = Utilities.base64Decode(base64Data);
    const blob = Utilities.newBlob(bytes, contentType, fileName);
    
    const folder = DriveApp.getFolderById(folderId);
    const file = folder.createFile(blob);
    file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
    
    // 2. Supabase 등록 (syncSvgLibrary 로직 재사용 가능하지만 즉시 반영을 위해 직접 등록)
    const fileId = file.getId();
    const blockName = fileName.replace('.svg', '');
    const publicUrl = `https://lh3.googleusercontent.com/d/${fileId}`;
    
    if (!shouldSkipDb) {
      const payload = [{ block_name: blockName, file_id: fileId, file_url: publicUrl, updated_at: new Date().toISOString() }];
      const headers = { 'Prefer': 'resolution=merge-duplicates' };
      callSupabase('svg_library', 'post', payload, headers);
    }
    
    return { success: true, message: 'SVG 업로드 완료', fileId: fileId, fileUrl: publicUrl };
  } catch (e) { return { success: false, error: 'SVG 업로드 실패: ' + e.toString() }; }
}

// [SVG 관리] 구글 드라이브 폴더 스캔 -> Supabase 라이브러리 업데이트 (스마트 동기화)
function syncSvgLibrary(continuationToken) {
  try {
    const startTime = new Date().getTime();
    const TIME_LIMIT = 210 * 1000; // 3.5분 제한 (Apps Script 최대 실행 시간 고려)

    // 1. Supabase에서 기존 데이터 가져오기 (중복 처리 최소화)
    const dbData = callSupabase('svg_library?select=block_name,file_id', 'get'); // 전체 목록 로드 (캐싱 역할)
    const dbMap = new Map();
    if (Array.isArray(dbData)) {
      dbData.forEach(row => dbMap.set(row.block_name, row.file_id));
    }

    const folder = DriveApp.getFolderById(CONFIG.SVG_FOLDER_ID);
    const files = folder.getFiles();
    // 이어달리기 토큰이 있으면 해당 지점부터 시작
    const iterator = continuationToken ? DriveApp.continueFileIterator(continuationToken) : folder.getFiles();

    // [수정] 중복 방지를 위해 배열 대신 Map 사용 (block_name을 키로 사용)
    const payloadMap = new Map();
    let processedCount = 0;
    
    while (iterator.hasNext()) {
      if (new Date().getTime() - startTime > TIME_LIMIT) {
        // 시간 초과 시 현재까지의 작업 저장 후 토큰 반환
        if (payloadMap.size > 0) {
          const headers = { 'Prefer': 'resolution=merge-duplicates' };
          // Map을 배열로 변환하여 전송
          callSupabase('svg_library', 'post', Array.from(payloadMap.values()), headers);
        }
        return { 
          success: true, 
          finished: false, 
          continuationToken: iterator.getContinuationToken(), 
          processedCount: processedCount, 
          message: `동기화 진행 중... (${processedCount}개 처리)` 
        };
      }

      const file = iterator.next();
      const fileName = file.getName();
      
      // 확장자가 .svg인 파일만 처리
      if (fileName.toLowerCase().endsWith('.svg')) {
        const blockName = fileName.substring(0, fileName.lastIndexOf('.'));
        const fileId = file.getId();
        
        // DB에 없거나, 파일 ID가 변경된(덮어쓰기 된) 경우에만 업데이트
        if (!dbMap.has(blockName) || dbMap.get(blockName) !== fileId) {
          
          // [중요] QGIS 접근용 권한 설정 (새 파일일 때만 수행하여 속도 향상)
          file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);

          const publicUrl = `https://lh3.googleusercontent.com/d/${fileId}`;
          
          // [수정] Map에 저장하여 같은 block_name이 나오면 덮어쓰기 (중복 제거)
          // 구글 드라이브에 같은 이름의 파일이 여러 개일 경우, 나중에 읽힌 파일(또는 로직에 따라) 하나만 전송됨
          payloadMap.set(blockName, {
            block_name: blockName,
            file_id: fileId,
            file_url: publicUrl,
            updated_at: new Date().toISOString()
          });
          processedCount++;
        }
      }
    }

    if (payloadMap.size > 0) {
      const headers = { 'Prefer': 'resolution=merge-duplicates' };
      callSupabase('svg_library', 'post', Array.from(payloadMap.values()), headers);
    }

    return { success: true, finished: true, processedCount: processedCount, message: `SVG 동기화 완료 (${processedCount}개 처리)` };
  } catch (e) { return { success: false, error: 'SVG 동기화 오류: ' + e.toString() }; }
}

// [설정] Supabase 설정 반환
function getSupabaseConfig() {
  try {
    const props = PropertiesService.getScriptProperties();
    const url = props.getProperty('SUPABASE_URL');
    const key = props.getProperty('SUPABASE_KEY');
    const vworldKey = props.getProperty('VWORLD_API_KEY'); // [추가] 브이월드 키
    
    if (!url || !key) return { success: false, error: 'Supabase 설정이 스크립트 속성에 없습니다.' };
    
    return { success: true, url: url, key: key, vworldKey: vworldKey };
  } catch (e) { return { success: false, error: e.toString() }; }
}

// [Supabase] 일시 정지 방지용 자동 접속 함수
// 이 함수를 앱스스크립트 트리거로 설정하여 매일 한 번씩 실행되게 하세요.
function keepSupabaseAlive() {
  try {
    // 프로젝트 테이블에서 데이터 1개만 조회하여 DB 활동 기록을 남김
    const result = callSupabase('projects?select=id&limit=1', 'get');
    Logger.log('Supabase Keep-Alive Ping: ' + (Array.isArray(result) ? 'Success' : 'Failed'));
  } catch (e) {
    Logger.log('Supabase Keep-Alive Error: ' + e.toString());
  }
}

// ==========================================
// Supabase 통신 헬퍼 함수
// ==========================================
function callSupabase(endpoint, method, payload, extraHeaders) {
  try {
    const baseUrl = PropertiesService.getScriptProperties().getProperty('SUPABASE_URL');
    const apiKey = PropertiesService.getScriptProperties().getProperty('SUPABASE_KEY');
    
    if (!baseUrl || !apiKey) throw new Error('Supabase 설정이 누락되었습니다. 스크립트 속성을 확인하세요.');

    const url = `${baseUrl}/rest/v1/${endpoint}`;
    const options = {
      method: method || 'get',
      headers: {
        'apikey': apiKey,
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'Prefer': 'return=representation' // 데이터 생성/수정 시 결과를 바로 반환받음
      },
      muteHttpExceptions: true // 에러 발생 시 예외를 던지지 않고 응답 코드를 확인
    };

    // 추가 헤더가 있으면 병합
    if (extraHeaders) {
      Object.assign(options.headers, extraHeaders);
    }

    if (payload) options.payload = JSON.stringify(payload);

    const response = UrlFetchApp.fetch(url, options);
    const responseCode = response.getResponseCode();
    const responseBody = response.getContentText();

    if (responseCode >= 200 && responseCode < 300) {
      // 내용이 없으면(204 No Content 등) 빈 객체 반환
      return responseBody ? JSON.parse(responseBody) : { success: true };
    } else {
      let errorMsg = `Supabase Error (${responseCode}): ${responseBody}`;
      if (responseCode === 401) errorMsg += " (스크립트 속성의 SUPABASE_KEY를 확인하세요. service_role 키 권장)";
      throw new Error(errorMsg);
    }
  } catch (e) {
    Logger.log('Supabase Call Error: ' + e.toString());
    throw e;
  }
}

// ==========================================
// GitHub Action 트리거 함수
// ==========================================
function triggerGitHubAction(projectId) {
  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('GITHUB_TOKEN');      // GitHub Personal Access Token
  const owner = props.getProperty('GITHUB_REPO_OWNER'); // GitHub 사용자 ID
  const repo = props.getProperty('GITHUB_REPO_NAME');   // 저장소 이름 (예: Asin-webapp)

  if (!token || !owner || !repo) {
    return { success: false, error: 'GitHub 설정(TOKEN, OWNER, NAME)이 스크립트 속성에 없습니다.' };
  }

  const url = `https://api.github.com/repos/${owner}/${repo}/dispatches`;
  const payload = {
    event_type: "convert_cad", // .yml 파일의 types와 일치해야 함
    client_payload: {
      project_id: projectId
    }
  };

  const options = {
    method: "post",
    headers: {
      "Authorization": `Bearer ${token}`,
      "Accept": "application/vnd.github.v3+json"
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  try {
    const res = UrlFetchApp.fetch(url, options);
    // 204 No Content가 성공 응답임
    if (res.getResponseCode() === 204) {
      return { success: true, message: 'GitHub Action이 시작되었습니다. (완료까지 약 1~2분 소요)' };
    } else {
      return { success: false, error: `GitHub 요청 실패 (${res.getResponseCode()}): ${res.getContentText()}` };
    }
  } catch (e) {
    return { success: false, error: 'GitHub 통신 오류: ' + e.toString() };
  }
}
