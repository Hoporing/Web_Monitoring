import time
import socket
import netifaces
import ipaddress
import threading
import os
import subprocess
import atexit
import hashlib
import tempfile
import requests as http
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, Response, render_template, request, jsonify, session
from flask_sock import Sock
from onvif import ONVIFCamera
from zeep.transports import Transport
import websocket as wsclient
import secrets

app = Flask(__name__)
sock = Sock(app)

# 서버 사이드 녹화 임시 파일 저장 경로 (None이면 시스템 temp 폴더 사용)
RECORD_TMP_DIR = r'D:\Temporary'

# 서버 사이드 녹화 상태
# {session_id: {'file': file_obj, 'path': str, 'active': bool}}
_recordings = {}
_init_segments = {}  # session_id -> bytes (fMP4 초기화 세그먼트)
_pending_downloads = {}  # session_id -> {'path': str, 'filename': str}
_rec_lock = threading.Lock()

# secret_key를 파일에 저장해 재시작 후에도 세션 유지
_secret_key_file = os.path.join(os.path.dirname(__file__), '.secret_key')
if os.path.exists(_secret_key_file):
    with open(_secret_key_file, 'r') as f:
        app.secret_key = f.read().strip()
else:
    app.secret_key = secrets.token_hex(32)
    with open(_secret_key_file, 'w') as f:
        f.write(app.secret_key)

# go2rtc
GO2RTC_URL = 'http://127.0.0.1:1984'
_go2rtc_proc = None

# 세션별 스트림 관리
# 구조: {session_id: {'stream_info': {...}, 'last_access': timestamp}}
active_sessions = {}
sessions_lock = threading.Lock()

# 검색 진행 상태 저장
scan_progress = {
    'total': 0,
    'scanned': 0,
    'found': 0,
    'is_scanning': False,
    'cameras': []
}
scan_progress_lock = threading.Lock()


# ── go2rtc 관리 ──────────────────────────────────────────────────────────────

def start_go2rtc():
    """go2rtc 프로세스 시작. 이미 실행 중이면 그냥 반환."""
    global _go2rtc_proc
    base = os.path.dirname(os.path.abspath(__file__))
    exe = os.path.join(base, 'go2rtc.exe')
    cfg = os.path.join(base, 'go2rtc.yaml')

    if not os.path.exists(exe):
        print("[go2rtc] go2rtc.exe를 프로젝트 폴더에 넣어주세요.")
        print("[go2rtc] 다운로드: https://github.com/AlexxIT/go2rtc/releases")
        return False

    _go2rtc_proc = subprocess.Popen(
        [exe, '-config', cfg],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    # API 응답 대기 (최대 5초)
    for _ in range(10):
        try:
            http.get(f'{GO2RTC_URL}/api/streams', timeout=1)
            print("[go2rtc] 시작 완료")
            return True
        except Exception:
            time.sleep(0.5)

    print("[go2rtc] 시작 실패 - API 응답 없음")
    return False


def stop_go2rtc():
    if _go2rtc_proc:
        _go2rtc_proc.terminate()


atexit.register(stop_go2rtc)


def add_go2rtc_stream(name, rtsp_url):
    """go2rtc에 RTSP 스트림 등록"""
    try:
        resp = http.put(
            f'{GO2RTC_URL}/api/streams',
            params={'name': name, 'src': rtsp_url},
            timeout=5
        )
        return resp.ok
    except Exception as e:
        print(f"[go2rtc] 스트림 등록 오류: {e}")
        return False


def remove_go2rtc_stream(name):
    """go2rtc에서 스트림 제거"""
    try:
        http.delete(
            f'{GO2RTC_URL}/api/streams',
            params={'name': name},
            timeout=5
        )
    except Exception as e:
        print(f"[go2rtc] 스트림 제거 오류: {e}")


def rtsp_to_stream_name(rtsp_url):
    """RTSP URL → 고정 스트림 이름 (동일 카메라 = 동일 이름, 16자 hex)"""
    return hashlib.md5(rtsp_url.encode()).hexdigest()[:16]


# ── ONVIF 검색 ───────────────────────────────────────────────────────────────

def get_local_subnets():
    """PC에 등록된 모든 네트워크 인터페이스의 서브넷 정보 가져오기"""
    subnets = []
    try:
        for interface in netifaces.interfaces():
            addrs = netifaces.ifaddresses(interface)
            if netifaces.AF_INET in addrs:
                for addr_info in addrs[netifaces.AF_INET]:
                    ip = addr_info.get('addr')
                    netmask = addr_info.get('netmask')
                    if ip and netmask and ip.startswith('192.'):
                        try:
                            network = ipaddress.IPv4Network(f"{ip}/{netmask}", strict=False)
                            subnet_info = {
                                'network': str(network.network_address),
                                'netmask': netmask,
                                'cidr': str(network),
                                'interface': interface,
                                'local_ip': ip
                            }
                            if not any(s['cidr'] == subnet_info['cidr'] for s in subnets):
                                subnets.append(subnet_info)
                        except Exception as e:
                            print(f"서브넷 계산 오류 ({ip}/{netmask}): {e}")
    except Exception as e:
        print(f"네트워크 인터페이스 조회 오류: {e}")
    return subnets


def check_onvif_device(ip, port=80, timeout=2):
    """특정 IP가 ONVIF 장치인지 확인"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((ip, port))
        sock.close()

        if result == 0:
            try:
                _transport = Transport(timeout=timeout, operation_timeout=timeout)
                cam = ONVIFCamera(ip, port, '', '', no_cache=True, transport=_transport)
                device_service = cam.create_devicemgmt_service()
                device_info = device_service.GetDeviceInformation()
                return {
                    'ip': ip,
                    'name': f"{device_info.Manufacturer} {device_info.Model}",
                    'port': port
                }
            except Exception:
                return {
                    'ip': ip,
                    'name': 'ONVIF Device (Authentication Required)',
                    'port': port
                }
    except Exception:
        pass
    return None


def scan_subnet_for_cameras(subnet_cidr, max_workers=50):
    """특정 서브넷에서 ONVIF 카메라 스캔"""
    cameras = []
    try:
        network = ipaddress.IPv4Network(subnet_cidr, strict=False)
        ip_list = [str(ip) for ip in network.hosts()]
        print(f"서브넷 {subnet_cidr} 스캔 시작 ({len(ip_list)} IPs)")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_ip = {executor.submit(check_onvif_device, ip): ip for ip in ip_list}
            for future in as_completed(future_to_ip):
                result = future.result()
                with scan_progress_lock:
                    scan_progress['scanned'] += 1
                    if result:
                        cameras.append(result)
                        scan_progress['found'] += 1
                        scan_progress['cameras'].append(result)
                        print(f"카메라 발견: {result['ip']} - {result['name']}")

        print(f"서브넷 {subnet_cidr} 스캔 완료: {len(cameras)}대 발견")
    except Exception as e:
        print(f"서브넷 스캔 오류 ({subnet_cidr}): {e}")
    return cameras


def discover_onvif_cameras():
    """PC의 모든 네트워크 서브넷에서 ONVIF 카메라 검색"""
    all_cameras = []
    try:
        with scan_progress_lock:
            scan_progress['is_scanning'] = True
            scan_progress['scanned'] = 0
            scan_progress['found'] = 0
            scan_progress['cameras'] = []

        subnets = get_local_subnets()
        if not subnets:
            print("검색할 네트워크 인터페이스가 없습니다.")
            with scan_progress_lock:
                scan_progress['is_scanning'] = False
            return []

        total_ips = 0
        for subnet in subnets:
            network = ipaddress.IPv4Network(subnet['cidr'], strict=False)
            total_ips += network.num_addresses - 2
        with scan_progress_lock:
            scan_progress['total'] = total_ips

        print(f"검색할 서브넷: {[s['cidr'] for s in subnets]} (총 {total_ips} IPs)")

        with ThreadPoolExecutor(max_workers=len(subnets)) as executor:
            future_to_subnet = {
                executor.submit(scan_subnet_for_cameras, subnet['cidr']): subnet
                for subnet in subnets
            }
            for future in as_completed(future_to_subnet):
                subnet = future_to_subnet[future]
                try:
                    cameras = future.result()
                    if cameras:
                        all_cameras.extend(cameras)
                except Exception as e:
                    print(f"서브넷 {subnet['cidr']} 스캔 실패: {e}")

        unique_cameras = []
        seen_ips = set()
        for cam in all_cameras:
            if cam['ip'] not in seen_ips:
                unique_cameras.append(cam)
                seen_ips.add(cam['ip'])

        unique_cameras.sort(key=lambda x: tuple(map(int, x['ip'].split('.'))))

        with scan_progress_lock:
            scan_progress['cameras'] = unique_cameras
            scan_progress['is_scanning'] = False

        print(f"총 {len(unique_cameras)}대의 카메라 발견")
        return unique_cameras

    except Exception as e:
        print(f"Discovery 오류: {e}")
        with scan_progress_lock:
            scan_progress['is_scanning'] = False
        return []


def get_profiles_from_onvif(ip, port, username, password, timeout=10):
    """ONVIF를 통해 모든 프로필 정보 조회"""
    try:
        transport = Transport(timeout=timeout, operation_timeout=timeout)
        cam = ONVIFCamera(ip, port, username, password, transport=transport)
        media_service = cam.create_media_service()
        profiles = media_service.GetProfiles()

        if not profiles:
            return None

        profile_list = []
        for idx, profile in enumerate(profiles):
            profile_info = {
                'index': idx,
                'name': profile.Name,
                'token': profile.token
            }

            if hasattr(profile, 'VideoEncoderConfiguration') and profile.VideoEncoderConfiguration:
                video_config = profile.VideoEncoderConfiguration
                if hasattr(video_config, 'Resolution') and video_config.Resolution:
                    profile_info['width'] = video_config.Resolution.Width
                    profile_info['height'] = video_config.Resolution.Height
                if hasattr(video_config, 'RateControl') and video_config.RateControl:
                    profile_info['fps'] = video_config.RateControl.FrameRateLimit

            stream_uri = media_service.GetStreamUri({
                'StreamSetup': {
                    'Stream': 'RTP-Unicast',
                    'Transport': {'Protocol': 'RTSP'}
                },
                'ProfileToken': profile.token
            })
            profile_info['rtsp_url'] = stream_uri.Uri
            profile_list.append(profile_info)

        return profile_list
    except Exception as e:
        err = str(e)
        if 'timeout' in err.lower() or 'timed out' in err.lower():
            print(f"ONVIF 타임아웃 ({ip}:{port}): {e}")
            return 'timeout'
        print(f"ONVIF 오류: {e}")
        return None


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    if 'session_id' not in session:
        session['session_id'] = secrets.token_hex(16)
    session_id = session['session_id']

    with sessions_lock:
        if session_id in active_sessions:
            sess = active_sessions[session_id]
            return render_template('index.html',
                                   stream_name=sess['stream_name'],
                                   stream_info=sess['stream_info'])
    return render_template('setup.html')


@app.route('/setup')
def setup():
    return render_template('setup.html')


@app.route('/api/discover', methods=['POST'])
def discover_cameras():
    """네트워크에서 ONVIF 카메라 검색 (백그라운드에서 실행)"""
    with scan_progress_lock:
        if scan_progress['is_scanning']:
            return jsonify({'error': '이미 검색이 진행 중입니다.'}), 400

    thread = threading.Thread(target=discover_onvif_cameras, daemon=True)
    thread.start()
    return jsonify({'success': True, 'message': '검색이 시작되었습니다.'})


@app.route('/api/scan_progress', methods=['GET'])
def get_scan_progress():
    """스캔 진행 상태 조회"""
    with scan_progress_lock:
        total = scan_progress['total']
        scanned = scan_progress['scanned']
        found = scan_progress['found']
        is_scanning = scan_progress['is_scanning']
        cameras = list(scan_progress['cameras'])

    percentage = int((scanned / total) * 100) if total > 0 else 0
    return jsonify({
        'is_scanning': is_scanning,
        'total': total,
        'scanned': scanned,
        'found': found,
        'percentage': percentage,
        'cameras': cameras
    })


@app.route('/api/get_profiles', methods=['POST'])
def get_profiles():
    """카메라의 사용 가능한 프로필 목록 가져오기"""
    data = request.get_json(silent=True) or {}
    ip = data.get('ip', '').strip()
    username = data.get('username', '').strip()
    password = data.get('password', '')
    try:
        port = int(data.get('port', 80))
    except (ValueError, TypeError):
        port = 80

    if not all([ip, username, password]):
        return jsonify({'error': '모든 필드를 입력해주세요.'}), 400

    profiles = get_profiles_from_onvif(ip, port, username, password)

    if profiles == 'timeout':
        return jsonify({'error': f'카메라 연결 시간 초과 (10초). IP({ip})와 포트({port})를 확인해주세요.'}), 408
    if not profiles:
        return jsonify({'error': 'ONVIF 연결 실패. IP, 포트, 인증 정보를 확인해주세요.'}), 400

    profile_list = []
    for profile in profiles:
        profile_list.append({
            'index': profile['index'],
            'name': profile['name'],
            'width': profile.get('width', 'N/A'),
            'height': profile.get('height', 'N/A'),
            'fps': profile.get('fps', 'N/A')
        })

    return jsonify({'success': True, 'profiles': profile_list})


@app.route('/api/connect', methods=['POST'])
def connect_camera():
    """카메라 RTSP를 go2rtc에 등록하고 세션 생성"""
    if 'session_id' not in session:
        session['session_id'] = secrets.token_hex(16)
    session_id = session['session_id']

    data = request.get_json(silent=True) or {}
    ip = data.get('ip', '').strip()
    username = data.get('username', '').strip()
    password = data.get('password', '')
    try:
        port = int(data.get('port', 80))
        profile_index = int(data.get('profile_index', 0))
    except (ValueError, TypeError):
        port = 80
        profile_index = 0

    if not all([ip, username, password]):
        return jsonify({'error': '모든 필드를 입력해주세요.'}), 400

    profiles = get_profiles_from_onvif(ip, port, username, password)

    if profiles == 'timeout':
        return jsonify({'error': f'카메라 연결 시간 초과. IP({ip}), 포트({port}) 확인해주세요.'}), 408
    if not profiles or profile_index >= len(profiles):
        return jsonify({'error': 'ONVIF 연결 실패 또는 잘못된 프로필 선택.'}), 400

    selected = profiles[profile_index]
    rtsp_url = selected['rtsp_url']

    # RTSP URL에 인증 정보 삽입
    if '://' in rtsp_url:
        protocol, rest = rtsp_url.split('://', 1)
        if '@' not in rest:
            rtsp_url = f"{protocol}://{username}:{password}@{rest}"

    stream_name = rtsp_to_stream_name(rtsp_url)

    with sessions_lock:
        # 기존 세션이 있으면 해당 스트림 참조 해제
        if session_id in active_sessions:
            old_name = active_sessions[session_id]['stream_name']
            del active_sessions[session_id]
            # 다른 세션이 같은 스트림을 쓰지 않으면 go2rtc에서 제거
            if not any(s['stream_name'] == old_name for s in active_sessions.values()):
                remove_go2rtc_stream(old_name)

        # 같은 RTSP URL을 이미 다른 세션이 쓰고 있으면 go2rtc 재등록 불필요
        already_registered = any(s['stream_name'] == stream_name for s in active_sessions.values())

    if not already_registered:
        if not add_go2rtc_stream(stream_name, rtsp_url):
            return jsonify({'error': 'go2rtc 스트림 등록 실패. go2rtc.exe가 실행 중인지 확인해주세요.'}), 503

    stream_info = {
        'width': selected.get('width', 'N/A'),
        'height': selected.get('height', 'N/A'),
        'fps': selected.get('fps', 'N/A'),
        'name': selected.get('name', 'Unknown'),
        'ip': ip
    }

    with sessions_lock:
        active_sessions[session_id] = {
            'stream_name': stream_name,
            'stream_info': stream_info,
            'last_access': time.time()
        }

    print(f"[세션 {session_id[:8]}] 스트림 연결 ({ip}, go2rtc: {stream_name})")
    return jsonify({'success': True, 'stream_info': stream_info})


@app.route('/api/disconnect', methods=['POST'])
def disconnect_camera():
    """세션 스트림 종료. 마지막 세션이면 go2rtc 스트림도 제거."""
    session_id = session.get('session_id')
    if session_id:
        with sessions_lock:
            sess = active_sessions.pop(session_id, None)
            if sess:
                stream_name = sess['stream_name']
                # 같은 스트림을 쓰는 세션이 더 없으면 go2rtc에서 제거
                if not any(s['stream_name'] == stream_name for s in active_sessions.values()):
                    remove_go2rtc_stream(stream_name)
    return jsonify({'success': True})


@app.route('/api/record/start', methods=['POST'])
def record_start():
    session_id = session.get('session_id')
    if not session_id:
        return jsonify({'error': '세션 없음'}), 400

    with _rec_lock:
        if session_id in _recordings and _recordings[session_id]['active']:
            return jsonify({'error': '이미 녹화 중'}), 400

        tmp_dir = RECORD_TMP_DIR or tempfile.gettempdir()
        os.makedirs(tmp_dir, exist_ok=True)
        path = os.path.join(tmp_dir, f'cctv_{session_id[:8]}_{int(time.time())}.mp4')
        f = open(path, 'wb')
        # init segment 먼저 기록 (없으면 재생 불가)
        init_seg = _init_segments.get(session_id)
        if init_seg:
            f.write(init_seg)
        _recordings[session_id] = {'file': f, 'path': path, 'active': True}

    return jsonify({'success': True})


@app.route('/api/record/stop', methods=['POST'])
def record_stop():
    session_id = session.get('session_id')
    if not session_id:
        return jsonify({'error': '세션 없음'}), 400

    with _rec_lock:
        rec = _recordings.pop(session_id, None)

    if not rec:
        return jsonify({'error': '녹화 중 아님'}), 400

    rec['file'].close()
    ts = time.strftime('%Y%m%d_%H%M%S')
    filename = f'cctv_{ts}.mp4'

    with _rec_lock:
        _pending_downloads[session_id] = {'path': rec['path'], 'filename': filename}

    return jsonify({'success': True})


@app.route('/api/record/download', methods=['GET'])
def record_download():
    session_id = session.get('session_id')
    if not session_id:
        return jsonify({'error': '세션 없음'}), 400

    with _rec_lock:
        dl = _pending_downloads.pop(session_id, None)

    if not dl:
        return jsonify({'error': '다운로드할 파일 없음'}), 404

    path = dl['path']
    filename = dl['filename']

    if not os.path.exists(path):
        return jsonify({'error': '파일 없음'}), 404

    file_size = os.path.getsize(path)

    def generate():
        with open(path, 'rb') as f:
            while chunk := f.read(65536):
                yield chunk
        os.remove(path)

    return Response(
        generate(),
        mimetype='video/mp4',
        headers={
            'Content-Disposition': f'attachment; filename="{filename}"',
            'Content-Length': str(file_size),
        }
    )


@sock.route('/ws')
def ws_stream(ws):
    """go2rtc WebSocket MSE 스트림 프록시"""
    src = request.args.get('src', '')
    print(f"[WS] 연결 요청: src={src}")
    if not src:
        return

    try:
        go2rtc_ws = wsclient.create_connection(
            f'ws://127.0.0.1:1984/api/ws?src={src}',
            timeout=10
        )
    except Exception as e:
        print(f"[WS] go2rtc 연결 실패: {e}")
        return

    session_id = session.get('session_id', '')

    def relay_from_go2rtc():
        first_binary = True
        try:
            while True:
                data = go2rtc_ws.recv()
                if data is None:
                    break
                ws.send(data)
                if isinstance(data, bytes):
                    with _rec_lock:
                        # 첫 바이너리 = fMP4 init segment 저장
                        if first_binary and session_id:
                            _init_segments[session_id] = data
                        # 녹화 중이면 파일에 기록
                        rec = _recordings.get(session_id)
                        if rec and rec['active']:
                            rec['file'].write(data)
                    first_binary = False
        except Exception:
            pass

    relay_thread = threading.Thread(target=relay_from_go2rtc, daemon=True)
    relay_thread.start()

    try:
        while True:
            msg = ws.receive()
            if msg is None:
                break
            # 브라우저 → go2rtc 전달 (코덱 협상 JSON 등)
            if isinstance(msg, bytes):
                go2rtc_ws.send_binary(msg)
            else:
                go2rtc_ws.send(msg)
    except Exception:
        pass
    finally:
        go2rtc_ws.close()
        # WebSocket 종료 시 진행 중인 녹화 파일 닫기
        with _rec_lock:
            rec = _recordings.pop(session_id, None)
        if rec:
            try:
                rec['file'].close()
                os.remove(rec['path'])
            except Exception:
                pass
        _init_segments.pop(session_id, None)


@app.route('/api/webrtc', methods=['POST'])
def webrtc_proxy():
    """브라우저 WebRTC SDP offer를 go2rtc로 프록시"""
    src = request.args.get('src', '')
    if not src:
        return 'src parameter required', 400

    try:
        resp = http.post(
            f'{GO2RTC_URL}/api/webrtc',
            params={'src': src},
            data=request.get_data(),
            headers={'Content-Type': 'application/sdp'},
            timeout=10
        )
        return Response(resp.content, mimetype='application/sdp', status=resp.status_code)
    except Exception as e:
        print(f"[WebRTC] 프록시 오류: {e}")
        return 'go2rtc 연결 실패. go2rtc가 실행 중인지 확인해주세요.', 503


def _cleanup_old_recordings():
    """2시간 이상 된 녹화 임시 파일 주기적 삭제"""
    while True:
        time.sleep(3600)  # 1시간마다 실행
        cutoff = time.time() - 7200  # 2시간
        for tmp in set(filter(None, [RECORD_TMP_DIR, tempfile.gettempdir()])):
            for fname in os.listdir(tmp):
                if fname.startswith('cctv_') and fname.endswith('.mp4'):
                    path = os.path.join(tmp, fname)
                    try:
                        if os.path.getmtime(path) < cutoff:
                            os.remove(path)
                            print(f'[녹화] 오래된 임시 파일 삭제: {fname}')
                    except Exception:
                        pass


if __name__ == "__main__":
    start_go2rtc()
    threading.Thread(target=_cleanup_old_recordings, daemon=True).start()
    app.run(host='0.0.0.0', port=80, threaded=True)
