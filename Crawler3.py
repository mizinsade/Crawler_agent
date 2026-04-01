import requests
from dateutil import parser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import trafilatura
from trafilatura.meta import reset_caches
import gc
from urllib.parse import urljoin, urlparse, urlsplit
import multiprocessing
import threading
import time
import re
import json
import os
import sys
import signal
import subprocess
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from queue import Empty
sys.path.append("/home/mizin/")
from NexusDB.NexusCore import NexusCore

URL_FIND_PROCESS_WORKER = 1
CONTETN_EXTRACT_PROCESS_WORKER = 200
DATABASE_DIR = "/home/mizin/llm_info_db3"
CHECKPOINT_FILE = os.path.join(DATABASE_DIR , "crawler_checkpoint3.json")
START_URL = ["https://news.naver.com/", "https://www.bbc.com/news", "https://www.ft.com/world"]

global processed_count
processed_count = 1

# --- 1번 프로세스: DB 저장 및 통계 출력 ---
def db_saver_process(data_queue, stop_event):
    print("[Process 1] DB Saver 가동 중...")
    core = NexusCore(base_dir=DATABASE_DIR)
    core.close()
    core = NexusCore(base_dir=DATABASE_DIR)
    start_time = time.time()
    total_saved = 0
    
    try:
        # 종료 신호가 와도 큐가 빌 때까지는 계속 저장
        while True:#not (stop_event.is_set() and data_queue.empty()):
            try:
                # data 구조: (url, content, q2_wait_time, p3_put_time)
                data = data_queue.get(timeout=1)
                if data is None:
                    print("[Process 1] 종료 신호를 수신했습니다. 잔여 데이터를 정리합니다.")
                    break
                url, content,metadata ,q2_wait, p3_put_time = data
                
                q3_wait = time.time() - p3_put_time
                
                if content:
                    if metadata:
                        # metadata = {}
                        # metadata['description'] = description
                        core.put(url, content,metadata=metadata)
                    else:
                        core.put(url, content)
                    # core.put(url, content)
                    total_saved += 1
                    
                    if total_saved % 10 == 0:
                        elapsed = time.time() - start_time
                        iph = (total_saved / elapsed) * 3600
                        now = datetime.now()
                        formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
                        print(f"\n" + "="*50)
                        print(f" [{formatted_time}]")
                        print(f" [REPORT] 저장: {total_saved}개 | 속도: {iph:.1f} items/h")
                        print(f" [LATENCY] P2->P3: {q2_wait:.4f}s | P3->P1: {q3_wait:.4f}s")
                        print(f" [LATEST] {url[:50]}...")
                        print("="*50)
                else:
                    now = datetime.now()
                    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    print(f"[{formatted_time}][DB] 내용 없음 스킵: {url}...")
            except Empty:
                continue
    finally:
        core.close()
        now = datetime.now()
        formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{formatted_time}][Process 1] DB 안전 종료 완료.")

# def db_saver_process(data_queue, stop_event):
#     now = datetime.now()
#     formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
#     print(f"[{formatted_time}][Process 1] DB Saver (Bulk Mode) 가동 중...")
#     core = NexusCore(base_dir="/home/mizin/llm_info_db1")
    
#     start_time = time.time()
#     total_saved = 0
#     bulk_buffer = []  # 데이터를 모아둘 리스트
#     BULK_SIZE = 100  # 100개씩 모아서 저장
    
#     try:
#         while not (stop_event.is_set() and data_queue.empty()):
#             try:
#                 # 1. 큐에서 데이터 가져오기
#                 data = data_queue.get(timeout=1)
#                 url, content, description,q2_wait, p3_put_time = data
                
#                 if content:
#                     bulk_buffer.append((url, content, description))
#                 else:
#                     print(f"[DB 저장 실패] content empty : {url}")
                
#                 # 2. 버퍼가 찼거나, 종료 신호가 왔을 때 저장
#                 if len(bulk_buffer) >= BULK_SIZE or (stop_event.is_set() and not data_queue.empty()):
#                     if bulk_buffer:
#                         # NexusCore에 bulk_put 같은 메서드가 있다면 최적이지만, 
#                         # 없다면 루프로 돌려도 내부적인 트랜잭션 처리가 효율적입니다.
#                         for b_url, b_content, b_description in bulk_buffer:
#                             if b_content:
#                                 if b_description:
#                                     metadata = {}
#                                     metadata['description'] = b_description
#                                     core.put(b_url, b_content,metadata=metadata)
#                                 else:
#                                     core.put(b_url, b_content)
                        
#                         total_saved += len(bulk_buffer)
                        
#                         # 통계 출력 (현재 시간당 속도 계산)
#                         elapsed = time.time() - start_time
#                         iph = (total_saved / elapsed) * 3600
#                         now = datetime.now()
#                         formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")

#                         print(f"[{formatted_time}] [BULK SAVE] {len(bulk_buffer)}개 저장 완료 | 누적: {total_saved} | 속도: {iph:.1f}/h")
                        
#                         bulk_buffer = [] # 버퍼 비우기

#             except Empty:
#                 # 큐가 비었더라도 버퍼에 남은 게 있다면 저장 (데이터 유실 방지)
#                 if bulk_buffer:
#                     for b_url, b_content, b_description in bulk_buffer:
#                         if b_description:
#                             metadata = {}
#                             metadata['description'] = b_description
#                             core.put(b_url, b_content,metadata=metadata)
#                         else:
#                             core.put(b_url, b_content)
#                     total_saved += len(bulk_buffer)
#                     bulk_buffer = []
#                 continue
#     finally:
#         # 최종 남은 데이터 처리
#         if bulk_buffer:
#             for b_url, b_content, b_description in bulk_buffer:
#                 if b_description:
#                     metadata = {}
#                     metadata['description'] = b_description
#                     core.put(b_url, b_content,metadata=metadata)
#                 else:
#                     core.put(b_url, b_content)
#         core.close()
#         now = datetime.now()
#         formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
#         print(f"[{formatted_time}][Process 1] DB 안전 종료.")

# --- 2번 프로세스용 함수들 ---
def fetch_links(url):
    found_urls = set()
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...'}
    try:
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            for a_tag in soup.find_all('a', href=True):
                o = urlsplit(a_tag['href'])
                if o.scheme in ['https', 'http']:
                    found_urls.add(a_tag['href'])
                else:
                    full_url = urljoin(url, a_tag['href'])
                    parsed = urlparse(full_url)
                    if parsed.scheme in ('http', 'https'):
                        found_urls.add(full_url)
    except:
        pass
    return list(found_urls)

def save_checkpoint(visited, to_visit):
    data = {"visited": list(visited), "to_visit": list(to_visit)}
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    now = datetime.now()
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{formatted_time}][System] 체크포인트 저장 완료. (남은 URL: {len(to_visit)}개)")

def load_checkpoint(default_urls):
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return set(data['visited']), data['to_visit']
        except:
            pass
    return set(), default_urls

def url_finder_process2(start_url, url_queue, stop_event):
    now = datetime.now()
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{formatted_time}][Process 2] URL Finder 시작")
    default_urls = start_url
    visited, to_visit_list = load_checkpoint(default_urls)
    
    # 리스트를 데크로 변환 (성능 최적화)
    from collections import deque
    to_visit = deque(to_visit_list)
    
    try:
        with ThreadPoolExecutor(max_workers=URL_FIND_PROCESS_WORKER) as executor:
            while not stop_event.is_set() and to_visit:
                
                # --- [추가] P2 잠시 멈춤 로직 (공급 조절) ---
                # P3가 아직 처리하지 못한 URL이 큐에 500개 이상 쌓여있다면 대기
                # 이 수치는 P3의 워커 수(50~100)의 5~10배 정도가 적당합니다.
                if url_queue.qsize() > 200:
                    # 큐가 비워질 때까지 2초씩 쉬면서 체크
                    # print(f"[P2] 대기 중... (현재 큐 잔량: {url_queue.qsize()}개)")
                    time.sleep(2)
                    continue
                # ------------------------------------------

                current_batch = []
                while to_visit and len(current_batch) < 10:
                    u = to_visit.popleft() # O(1) 성능
                    if u not in visited:
                        current_batch.append(u)
                        visited.add(u)

                if not current_batch:
                    time.sleep(1)
                    continue

                # URL 추출 및 큐에 넣기
                future_to_url = {executor.submit(fetch_links, url): url for url in current_batch}
                for future in future_to_url:
                    try:
                        new_urls = future.result()
                        for n_url in new_urls:
                            # (url, 현재시간)을 넣어야 P2->P3 지연시간 측정이 정확해집니다.
                            if n_url not in visited:
                                url_queue.put((n_url, time.time()))
                                to_visit.append(n_url)
                    except:
                        continue
    finally:
        save_checkpoint(visited, list(to_visit))
        now = datetime.now()
        formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{formatted_time}][Process 2] URL Finder 안전 종료.")
        print(f"[{formatted_time}][Process 2] 남은 URL {url_queue.qsize()}개.")

_SESSION = None

def get_session():
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        # 커넥션 풀 설정 (워커 수만큼 풀을 넉넉히 잡습니다)
        adapter = HTTPAdapter(
            pool_connections=CONTETN_EXTRACT_PROCESS_WORKER, 
            pool_maxsize=CONTETN_EXTRACT_PROCESS_WORKER,
            max_retries=Retry(total=3, backoff_factor=0.1)
        )
        _SESSION.mount('http://', adapter)
        _SESSION.mount('https://', adapter)
        _SESSION.headers.update({'User-Agent': 'Mozilla/5.0...'})
    return _SESSION

# --- 3번 프로세스: 콘텐츠 정제 ---
def process_content(url, p2_put_time, data_queue):
    session = get_session()
    soup = None
    html_text = ""

    try:

        # 1. HEAD 요청으로 메타데이터만 확인 (매우 빠름)
        # head_res = subprocess.run(
        #     ['curl', '-I', '-L', '--connect-timeout', '2', '--max-time', '10', '-A', 'Mozilla/5.0', url],
        #     capture_output=True, text=True, errors='ignore'
        # )
        # header = head_res.stdout.lower()
        # date_info = None
        # # HTML이 아니거나(예: 이미지), 용량이 너무 크면(예: 2MB 초과) 즉시 포기
        # if "content-type: text/html" not in header:
        #     return url, ""
        # session = get_session()

        # result = subprocess.run(
        #     ['curl', '-s', '-L', '--max-time', '5', '-A', 'Mozilla/5.0', url],
        #     capture_output=True, text=True, encoding='utf-8', errors='ignore'
        # )
        with session.get(url, timeout=5) as result:
            html_text = result.text
        # if result.status_code != 200: 
        #     print("wtf")
        #     return url, "", ""
        # if result.text: 
            # return url, "", ""

        soup = BeautifulSoup(html_text, 'lxml')
        # soup = BeautifulSoup(result.stdout, 'lxml')
        
        final_description = ""
        final_content = ""
        # Meta Description -> Title -> Longest Div 순서
        desc_tag = (soup.find("meta", attrs={"name": "description"}) or 
                    soup.find("meta", attrs={"property": "og:description"}) or
                    soup.find("meta", attrs={"property": "twitter:description"}))
        if desc_tag and desc_tag.get("content"):
            final_description = desc_tag["content"].strip()
        
        # if not final_content:
        #     title_tag = (soup.find("meta", attrs={"name": "title"}) or 
        #                  soup.find("meta", attrs={"property": "og:title"}) or
        #                  soup.find("meta", attrs={"property": "twitter:title"}))
        #     if title_tag and title_tag.get("content"):
        #         final_content = title_tag["content"].strip()

        # if not final_content:
        #     final_content = trafilatura.extract(result.stdout)
        # if not final_content:
        #     article = soup.find('article')
    
        #     if article:
        #         # 2. article 내부의 모든 텍스트 추출
        #         # separator=' ' : 각 태그 사이에 공백을 넣어 단어가 붙지 않게 함
        #         # strip=True : 앞뒤 공백 제거
        #         raw_text = article.get_text(separator=' ', strip=True)

        #         # 3. 연속된 공백이나 줄바꿈을 하나로 정리 (정규표현식)
        #         final_content = re.sub(r'\s+', ' ', raw_text).strip()

        if not final_content:
            final_content = trafilatura.extract(
            html_text, 
            no_fallback=True, 
            include_comments=False, 
            include_tables=False
        )
            # final_content = trafilatura.extract(result.stdout)

        if not final_content:
            for noisy in soup(["script", "style", "header", "footer", "nav", "aside", "form","br"]):
                noisy.decompose()
            divs = soup.find_all('div')
            best_div = max(divs, key=lambda d: len(d.get_text(strip=True)), default=None)
            if best_div:
                final_content = best_div.get_text(separator=' ', strip=True)

        date_tags = [
        ('property', 'article:published_time'),
        ('name', 'pubdate'),
        ('name', 'publishdate'),
        ('property', 'og:reg_date'),      # 일부 한국 언론사 전용
        ('name', 'dc.date.issued'),
        ('name', 'date'),
        ]

        publish_date = ""
        refine_date = ""
        for attr, value in date_tags:
            tag = soup.find('meta', {attr: value})
            if tag and tag.get('content'):
                publish_date = tag['content'].strip()

        if publish_date is not None and publish_date:
            try:
                dt = parser.parse(publish_date)
                refine_date = dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                refine_date = ""
                pass # 파싱 실패 시 원본 유지

        refined_content = re.sub(r'\s+', ' ', final_content).strip()
        refined_description = re.sub(r'\s+', ' ', final_description).strip()

        # 품질 검사
        # if len(refined_content) < 150:  # 너무 짧은 글 (로그인 창, 에러 메시지 등)
        #     return url, ""
        
        # 의미 없는 문구 필터 (예: "쿠키를 허용해 주세요", "로그인 후 이용 가능")
        # spam_keywords = ["javascript is disabled", "enable cookies", "access denied"]
        # if any(key in refined_content.lower() for key in spam_keywords):
        #     return url, ""
        metadata = {}
        if final_description is not None and final_description: metadata['description'] = re.sub(r'\s+', ' ', final_description).strip()
        if refine_date is not None and refine_date: metadata['publish_date'] = refine_date
        data_to_send_metadata = metadata if metadata else None

        data_queue.put((url, refined_content, data_to_send_metadata,time.time() - p2_put_time, time.time()))
        # return url, refined_content, refined_description
        # global processed_count
        # processed_count += 1
        # print(processed_count)
        return 1
    except Exception as e:
        # [중요] 예외 객체를 즉시 날려서 Traceback이 지역 변수를 붙잡지 못하게 함
        # print(f"[process 3]{e}")
        e = None 
        return 0#url, "", ""
    finally:
        # 3. [핵심] BeautifulSoup 트리의 순환 참조를 강제로 끊어 메모리 폭발 방지
        if soup:
            soup.decompose() 
            
        # 4. 남아있는 대용량 변수 명시적 삭제
        html_text = None
        soup = None

def content_extractor_process(url_queue, data_queue, stop_event):
    now = datetime.now()
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{formatted_time}][Process 3] Content Extractor 가동 중...")

        # 메모리 추적 시작
    # tracemalloc.start()
    max_queue_size = int(CONTETN_EXTRACT_PROCESS_WORKER * 1.5)
    semaphore = threading.Semaphore(max_queue_size)

    global processed_count

    with ThreadPoolExecutor(max_workers=CONTETN_EXTRACT_PROCESS_WORKER) as executor:
        while True:#not (stop_event.is_set() and url_queue.empty()):
            try:
                # 큐에서 (url, p2_put_time) 꺼냄
                target_url, p2_put_time = url_queue.get(timeout=1)
                # q2_wait_time = time.time() - p2_put_time
                
                future = executor.submit(process_content, target_url, p2_put_time, data_queue)
                # executor.submit(process_content, target_url, p2_put_time, data_queue)
                
                semaphore.acquire()
                
                # if processed_count % 1001 == 0 or processed_count % 1002 == 0 or processed_count % 1003 == 0 or processed_count % 1004 == 0 or processed_count % 1005 == 0 or processed_count % 1006 == 0 or processed_count % 1007 == 0 or processed_count % 1008 == 0 or processed_count % 1009 == 0:
                #     # 2. Process 3 내부에서 캐시 및 GC 청소 (여기서 해야 의미가 있습니다!)
                #     # reset_caches()
                #     # gc.collect()
                #     # 1. 범인 색출 로직
                #     snapshot = tracemalloc.take_snapshot()
                #     top_stats = snapshot.statistics('lineno')
                #     print("\n" + "="*50)
                #     print(f"[Process 3 메모리 누수 추적 - Top 5]")
                #     for stat in top_stats[:19]:
                #         print(stat)
                #     print("="*50 + "\n")
                if processed_count % 100 == 0:
                    reset_caches()
                    gc.collect()
                    now = datetime.now()
                    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    print(f"[{formatted_time}][System] 메모리 최적화")
                    processed_count = 1
                #     # 1. 범인 색출 로직
                #     snapshot = tracemalloc.take_snapshot()
                #     top_stats = snapshot.statistics('lineno')
                #     print("\n" + "="*50)
                #     print(f"[Process 3 메모리 누수 추적 - Top 5]")
                #     for stat in top_stats[:19]:
                #         print(stat)
                #     print("="*50 + "\n")

                def done_callback(fut):
                    num = fut.result()
                    global processed_count
                    processed_count += num
                    
                    semaphore.release()
                    # (url, content, P2대기시간, P3넣은시간)
                    # data_queue.put((res_url, res_content, res_description,q2_wait, time.time()))
                
                future.add_done_callback(done_callback)
            except Empty:
                if stop_event.is_set(): break
                continue
    data_queue.put(None)
    print("[Process 3] Content Extractor 안전 종료.")

def signal_handler(sig, frame, stop_event):
    now = datetime.now()
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{formatted_time}][System] 종료 신호 수신. 데이터를 정리하고 종료합니다...")
    stop_event.set()

signal.signal(signal.SIGINT, signal.SIG_IGN)

# --- 메인 실행부 ---
if __name__ == "__main__":
    multiprocessing.set_start_method('spawn') # DB 연결 안정성을 위해 권장
    
    url_queue = multiprocessing.Queue()
    data_queue = multiprocessing.Queue()
    stop_event = multiprocessing.Event()

    signal.signal(signal.SIGINT, lambda sig, frame: signal_handler(sig, frame, stop_event))
    signal.signal(signal.SIGTERM, lambda sig, frame: signal_handler(sig, frame, stop_event))

    # 프로세스 설정
    p1 = multiprocessing.Process(target=db_saver_process, args=(data_queue, stop_event))
    p2 = multiprocessing.Process(target=url_finder_process2, args=(START_URL, url_queue, stop_event))
    p3 = multiprocessing.Process(target=content_extractor_process, args=(url_queue, data_queue, stop_event))


    # 실행
    p1.start()
    p2.start()
    p3.start()

    # 순차적 종료 대기
    p2.join() # URL 수집 중단
    p3.join() # 남은 URL 추출 완료 대기
    p1.join() # 남은 데이터 저장 완료 대기

    now = datetime.now()
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{formatted_time}][System] 모든 작업이 안전하게 완료되었습니다.")
