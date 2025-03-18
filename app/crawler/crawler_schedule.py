import subprocess
import logging
import os
import time
from datetime import datetime, timedelta
import schedule
import sys

# 로깅 설정
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{log_dir}/crawler_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)

# 크롤러 목록 정의
hotissue_crawlers = [
    # "/code/app/crawler/hotissue/bobaedream_bestboard.py",
    # "/code/app/crawler/hotissue/dcinside_realtimebestboard.py",
    # "/code/app/crawler/hotissue/fmkorea_funnyboard.py",
    "/code/app/crawler/hotissue/inven_openissue.py",
    # "/code/app/crawler/hotissue/mlbpark_bullpen.py",
    # "/code/app/crawler/hotissue/ppomppu_freeboard.py",
    # "/code/app/crawler/hotissue/ruliweb_funnyboard.py"
]

politics_crawlers = [
    # "/code/app/crawler/politics/bobaedream_politics.py",
    # "/code/app/crawler/politics/dcinside_peoplepower.py",
    # "/code/app/crawler/politics/dcinside_politics.py",
    # "/code/app/crawler/politics/fmkorea_politics.py",
    # "/code/app/crawler/politics/mlbpark_politics.py",
    # "/code/app/crawler/politics/ppomppu_politics.py",
    # "/code/app/crawler/politics/ruliweb_politics.py",
    # "/code/app/crawler/politics/ruliweb_society_politics_ecomomy.py"
]

def run_crawler(script_path, timeout_seconds=1200):  # 기본 5분(300초) 타임아웃
    """단일 크롤러 실행 및 결과 반환 (타임아웃 적용)"""
    try:
        logging.info(f"크롤러 실행 중: {script_path}")
        # 크롤러 실행 및 출력 캡처 (타임아웃 설정)
        result = subprocess.run(
            ["python3", script_path], 
            capture_output=True, 
            text=True, 
            check=False,
            timeout=timeout_seconds  # 타임아웃 설정
        )
        
        # 종료 코드 확인
        if result.returncode == 0:
            logging.info(f"크롤러 성공: {script_path}")
        else:
            logging.error(f"크롤러 실패: {script_path}, 종료 코드: {result.returncode}")
            logging.error(f"오류 메시지: {result.stderr}")
        
        # 표준 출력 로깅
        if result.stdout:
            logging.info(f"{script_path} 출력:\n{result.stdout}")
            
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        logging.error(f"크롤러 타임아웃: {script_path}, {timeout_seconds}초 이상 실행되어 중단")
        return False
    except Exception as e:
        logging.error(f"크롤러 실행 중 예외 발생: {script_path}, 오류: {str(e)}")
        return False

def run_all_crawlers():
    """모든 크롤러 순차적으로 실행"""
    logging.info("=== 크롤링 작업 시작 ===")
    
    results = {
        "hotissue": {"success": 0, "failed": 0, "failed_list": [], "success_list": []},
        "politics": {"success": 0, "failed": 0, "failed_list": [], "success_list": []}
    }
    
    # 핫이슈 크롤러 실행
    logging.info("핫이슈 크롤러 실행 시작")
    for crawler in hotissue_crawlers:
        success = run_crawler(crawler)
        if success:
            results["hotissue"]["success"] += 1
            results["hotissue"]["success_list"].append(crawler)
        else:
            results["hotissue"]["failed"] += 1
            results["hotissue"]["failed_list"].append(crawler)
        time.sleep(30)
    
    # 정치 크롤러 실행
    logging.info("정치 크롤러 실행 시작")
    for crawler in politics_crawlers:
        success = run_crawler(crawler)
        if success:
            results["politics"]["success"] += 1
            results["politics"]["success_list"].append(crawler)
        else:
            results["politics"]["failed"] += 1
            results["politics"]["failed_list"].append(crawler)
        time.sleep(30)
    
    # 결과 요약
    logging.info("=== 크롤링 작업 완료 ===")
    logging.info(f"핫이슈: 성공 {results['hotissue']['success']}, 실패 {results['hotissue']['failed']}")
    logging.info(f"정치: 성공 {results['politics']['success']}, 실패 {results['politics']['failed']}")
    
    if results["hotissue"]["failed"] > 0:
        logging.info(f"실패한 핫이슈 크롤러: {', '.join(results['hotissue']['failed_list'])}")
    if results["politics"]["failed"] > 0:
        logging.info(f"실패한 정치 크롤러: {', '.join(results['politics']['failed_list'])}")
    
    # 성공한 크롤러에 대해서만 데이터 삽입
    for crawler in results["hotissue"]["success_list"]:
        try:
            import pandas as pd
            base_name = os.path.basename(crawler)
            csv_name = base_name.replace('.py', f'_{datetime.now().strftime("%Y%m%d")}.csv')
            base_data_folder = '/code/data'
            today_folder = os.path.join(base_data_folder, datetime.now().strftime('%Y%m%d'))
            csv_path = os.path.join(today_folder, csv_name)
            
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path, encoding='utf-8-sig')
                data = df.to_dict('records')
                insert_to_db(data, is_politics=False)
                logging.info(f"핫이슈 데이터 삽입 완료: {csv_path}")
        except Exception as e:
            logging.error(f"핫이슈 데이터 삽입 중 오류 발생: {str(e)}")

    for crawler in results["politics"]["success_list"]:
        try:
            base_name = os.path.basename(crawler)
            csv_name = base_name.replace('.py', f'_{datetime.now().strftime("%Y%m%d")}.csv')
            base_data_folder = '/code/data'
            today_folder = os.path.join(base_data_folder, datetime.now().strftime('%Y%m%d'))
            csv_path = os.path.join(today_folder, csv_name)
            
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path, encoding='utf-8-sig')
                data = df.to_dict('records')
                insert_to_db(data, is_politics=True)
                logging.info(f"정치 데이터 삽입 완료: {csv_path}")
        except Exception as e:
            logging.error(f"정치 데이터 삽입 중 오류 발생: {str(e)}")

# def run_all_crawlers():
#     """모든 크롤러 순차적으로 실행"""
#     logging.info("=== 크롤링 작업 시작 ===")
    
#     # 실행 결과 추적
#     results = {
#         "hotissue": {"success": 0, "failed": 0, "failed_list": [], "success_list": []},
#         "politics": {"success": 0, "failed": 0, "failed_list": [], "success_list": []}
#     }
    
#     # 핫이슈 크롤러 실행
#     logging.info("핫이슈 크롤러 실행 시작")
#     for crawler in hotissue_crawlers:
#         success = run_crawler(crawler)
#         if success:
#             results["hotissue"]["success"] += 1
#             results["hotissue"]["success_list"].append(crawler)
#         else:
#             results["hotissue"]["failed"] += 1
#             results["hotissue"]["failed_list"].append(crawler)
#         time.sleep(30)  # 크롤러 간 30초 간격
    
#     # 정치 크롤러 실행
#     logging.info("정치 크롤러 실행 시작")
#     for crawler in politics_crawlers:
#         success = run_crawler(crawler)
#         if success:
#             results["politics"]["success"] += 1
#             results["politics"]["success_list"].append(crawler)
#         else:
#             results["politics"]["failed"] += 1
#             results["politics"]["failed_list"].append(crawler)
#         time.sleep(30)  # 크롤러 간 30초 간격
    
#     # 결과 요약
#     logging.info("=== 크롤링 작업 완료 ===")
#     logging.info(f"핫이슈: 성공 {results['hotissue']['success']}, 실패 {results['hotissue']['failed']}")
#     logging.info(f"정치: 성공 {results['politics']['success']}, 실패 {results['politics']['failed']}")
    
#     if results["hotissue"]["failed"] > 0:
#         logging.info(f"실패한 핫이슈 크롤러: {', '.join(results['hotissue']['failed_list'])}")
#     if results["politics"]["failed"] > 0:
#         logging.info(f"실패한 정치 크롤러: {', '.join(results['politics']['failed_list'])}")
    
#     # 성공한 크롤러에 대해서만 데이터 삽입
#     for crawler in results["hotissue"]["success_list"]:
#         try:
#             # CSV 파일 경로 수정 (절대 경로 사용)
#             base_name = os.path.basename(crawler)
#             csv_name = base_name.replace('.py', f'_{datetime.now().strftime("%Y%m%d")}.csv')
#             base_data_folder = '/code/data'  # Docker 환경의 절대 경로
#             today_folder = os.path.join(base_data_folder, datetime.now().strftime('%Y%m%d'))
#             csv_path = os.path.join(today_folder, csv_name)
            
#             if os.path.exists(csv_path):
#                 import pandas as pd
#                 df = pd.read_csv(csv_path, encoding='utf-8-sig')
#                 data = df.to_dict('records')
#                 insert_to_db(data, is_politics=False)
#                 logging.info(f"핫이슈 데이터 삽입 완료: {csv_path}")
#         except Exception as e:
#             logging.error(f"핫이슈 데이터 삽입 중 오류 발생: {str(e)}")

#     for crawler in results["politics"]["success_list"]:
#         try:
#             # CSV 파일 경로 수정 (절대 경로 사용)
#             base_name = os.path.basename(crawler)
#             csv_name = base_name.replace('.py', f'_{datetime.now().strftime("%Y%m%d")}.csv')
#             base_data_folder = '/code/data'  # Docker 환경의 절대 경로
#             today_folder = os.path.join(base_data_folder, datetime.now().strftime('%Y%m%d'))
#             csv_path = os.path.join(today_folder, csv_name)
            
#             if os.path.exists(csv_path):
#                 import pandas as pd
#                 df = pd.read_csv(csv_path, encoding='utf-8-sig')
#                 data = df.to_dict('records')
#                 insert_to_db(data, is_politics=True)
#                 logging.info(f"정치 데이터 삽입 완료: {csv_path}")
#         except Exception as e:
#             logging.error(f"정치 데이터 삽입 중 오류 발생: {str(e)}")

def check_crawler_files():
    """크롤러 파일이 존재하는지 확인"""
    missing_files = []
    
    for crawler in hotissue_crawlers + politics_crawlers:
        if not os.path.exists(crawler):
            missing_files.append(crawler)
    
    if missing_files:
        logging.error(f"다음 크롤러 파일을 찾을 수 없습니다: {', '.join(missing_files)}")
        return False
    
    return True

import mysql.connector
from mysql.connector import Error
# import logging

# 로깅 설정 (기본 설정, 필요 시 파일로 출력하도록 조정 가능)
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def select_site_info(cursor, table_name, key_values, use_title_writer=False):
    """게시글 중복 체크 (post_id와 community 기준, 또는 title과 writer 기준)"""
    if use_title_writer:
        query = f"SELECT seq, reg_date, views, recommend, content, images FROM {table_name} WHERE title = %s AND writer = %s"
        logging.debug(f"Executing query (title, writer): {query} with values {key_values}")
        cursor.execute(query, key_values)
    else:
        query = f"SELECT seq, reg_date, views, recommend, content, images FROM {table_name} WHERE post_id = %s AND community = %s"
        logging.debug(f"Executing query (post_id, community): {query} with values {key_values}")
        cursor.execute(query, key_values)
    
    result = cursor.fetchone()
    if cursor.description:
        cursor.fetchall()  # 남은 결과 소비
    logging.debug(f"Query result: {result}")
    return result  # (seq, reg_date, views, recommend, content, images) or None

def insert_site_info(cursor, table_name, values):
    """새 게시글 삽입"""
    query = f"INSERT INTO {table_name} (post_id, community, category, title, link, writer, reg_date, views, recommend, content, images) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    logging.debug(f"Inserting new record with values: {values}")
    cursor.execute(query, values)

def update_site_info(cursor, table_name, values):
    """기존 게시글 업데이트"""
    query = f"UPDATE {table_name} SET reg_date = %s, views = %s, recommend = %s, content = %s, images = %s WHERE seq = %s"
    logging.debug(f"Updating record with values: {values}")
    cursor.execute(query, values)

def insert_to_db(data, is_politics=True):
    conn = None
    cursor = None
    
    try:
        import pandas as pd
        import json
        
        conn = mysql.connector.connect(
            host=os.environ.get('DB_HOST'),
            port=int(os.environ.get('DB_PORT')),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            database=os.environ.get('DB_NAME'),
            connection_timeout=10
        )
        cursor = conn.cursor()
        
        table_name = "pgm_current_site" if is_politics else "pgm_hot_site"

        for item in data:
            processed_item = {}
            for key, value in item.items():
                if pd.isna(value) or value in ('nan', 'NaN', 'None'):
                    processed_item[key] = None
                else:
                    processed_item[key] = value
            
            reg_date_value = processed_item.get('Date', datetime.now())
            if isinstance(reg_date_value, datetime):
                reg_date_value = reg_date_value.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(reg_date_value, str):
                try:
                    reg_date_value = datetime.strptime(reg_date_value, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        reg_date_value = datetime.strptime(reg_date_value, '%Y-%m-%d %H:%M').strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        reg_date_value = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            post_id = str(processed_item.get('Post ID', '')) if processed_item.get('Post ID') is not None else ''
            community = str(processed_item.get('Community', '')) if processed_item.get('Community') is not None else ''
            category = str(processed_item.get('Category', '')) if processed_item.get('Category') is not None else ''
            title = str(processed_item.get('Title', '')) if processed_item.get('Title') is not None else ''
            link = str(processed_item.get('Link', '')) if processed_item.get('Link') is not None else ''
            writer = str(processed_item.get('Writer', '')) if processed_item.get('Writer') is not None else ''
            views = int(processed_item.get('Views', 0)) if processed_item.get('Views') is not None else 0
            recommend = int(processed_item.get('Recommend', 0)) if processed_item.get('Recommend') is not None else 0
            content = str(processed_item.get('Content', '')) if processed_item.get('Content') is not None else ''
            
            images = processed_item.get('Images', [])
            if images is None:
                images_str = '[]'
            elif isinstance(images, str):
                images_str = images
            else:
                images_str = json.dumps(images, ensure_ascii=False)
            
            # 중복 체크 기준 결정
            use_title_writer = not post_id or post_id.strip() == ''
            if use_title_writer:
                if not title or not writer:
                    logging.warning(f"Title 또는 Writer가 비어 있음 - 삽입 건너뜀: {processed_item}")
                    continue
                key_values = (title, writer)
                logging.debug(f"Checking for existing record with Title: {title}, Writer: {writer}")
            else:
                key_values = (post_id, community)
                logging.debug(f"Checking for existing record with Post ID: {post_id}, Community: {community}")
            
            existing = select_site_info(cursor, table_name, key_values, use_title_writer=use_title_writer)
            
            if existing is None:
                logging.debug(f"No existing record found. Inserting new record: {processed_item}")
                insert_values = (post_id, community, category, title, link, writer, reg_date_value, views, recommend, content, images_str)
                insert_site_info(cursor, table_name, insert_values)
                logging.debug(f"Successfully inserted new record - Title: {title}, Writer: {writer}")
            
            elif isinstance(existing, tuple) and len(existing) >= 6:
                seq, existing_reg_date, existing_views, existing_recommend, existing_content, existing_images = existing
                logging.debug(f"Existing record found: (seq: {seq}, reg_date: {existing_reg_date}, views: {existing_views}, recommend: {existing_recommend}, content: {existing_content}, images: {existing_images})")
                logging.debug(f"New record data: (reg_date: {reg_date_value}, views: {views}, recommend: {recommend}, content: {content}, images: {images_str})")
                
                is_identical = (
                    reg_date_value == existing_reg_date and
                    str(views) == str(existing_views) and
                    str(recommend) == str(existing_recommend) and
                    content == existing_content and
                    images_str == existing_images and
                    category == str(processed_item.get('Category', '')) and
                    title == str(processed_item.get('Title', '')) and
                    link == str(processed_item.get('Link', '')) and
                    writer == str(processed_item.get('Writer', ''))
                )
                
                if is_identical:
                    logging.debug(f"Records are identical. Skipping - Title: {title}, Writer: {writer}")
                    continue
                else:
                    logging.debug(f"Records differ. Updating record with seq: {seq}")
                    update_values = (reg_date_value, views, recommend, content, images_str, seq)
                    update_site_info(cursor, table_name, update_values)
                    logging.debug(f"Successfully updated record - Title: {title}, Writer: {writer}")
            else:
                logging.error(f"Invalid existing data format: {existing}")
                continue
        
        conn.commit()
        logging.info(f"Transaction committed for table: {table_name}")
        return True
        
    except mysql.connector.Error as e:
        logging.error(f"DB 삽입 오류: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logging.error(f"예상치 못한 오류: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logging.debug("Connection and cursor closed")

# 매일 지정된 시간에 실행
schedule.every().day.at("05:00").do(run_all_crawlers)
schedule.every().day.at("11:00").do(run_all_crawlers)
schedule.every().day.at("17:00").do(run_all_crawlers)
schedule.every().day.at("23:00").do(run_all_crawlers)

# 시작 시 즉시 실행 (테스트용)
logging.info("스케줄러 시작됨 - 5시, 11시, 17시, 23시에 크롤러가 실행됩니다.")
logging.info("시작 시 즉시 크롤러를 실행합니다.")

# 파일 존재 여부 확인
if check_crawler_files():
    # 메인 루프
    try:
        # 시작 시 즉시 실행
        run_all_crawlers()
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # 1분마다 스케줄 확인
            except Exception as e:
                logging.error(f"스케줄러 실행 중 오류 발생: {str(e)}")
                time.sleep(300)  # 오류 발생 시 5분 대기 후 재시도
    except KeyboardInterrupt:
        logging.info("사용자에 의해 스케줄러가 중단되었습니다.")
        sys.exit(0)
else:
    logging.error("크롤러 파일이 존재하지 않아 종료합니다.")
    sys.exit(1)
