import pandas as pd
from datetime import datetime
import time
import requests
from bs4 import BeautifulSoup as Soup
import random
import html
import os

# 헤더 설정
def get_headers():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://bbs.ruliweb.com/"
    }

# 유효한 URL 확인
def is_valid_post_url(url):
    if not url or "javascript:" in url:
        return False
    return url.startswith("http")

# 게시글 내용 크롤링 (BeautifulSoup만 사용)
def get_post_content(post_url, delay=5):
    if not is_valid_post_url(post_url):
        print(f"유효하지 않은 URL 건너뜀: {post_url}")
        return {"text": "유효하지 않은 URL", "images": [], "recommend": "0", "actual_date": None}

    try:
        headers = get_headers()
        response = requests.get(post_url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        soup = Soup(response.text, "html.parser")
        print(f"크롤링 중: {post_url}")
    except Exception as e:
        print(f"게시글 페이지 로드 오류: {post_url} - {str(e)}")
        return {"text": f"로드 오류: {str(e)}", "images": [], "recommend": "0", "actual_date": None}

    # 게시글 실제 날짜 확인
    actual_date = None
    date_elem = soup.find("span", class_="regdate", itemprop="datePublished")
    if date_elem:
        date_str = date_elem.text.strip()
        try:
            # "2025.03.12 (13:52:47)" 형식 파싱
            date_parts = date_str.split(" (")
            date_part = date_parts[0]
            time_part = date_parts[1].rstrip(")")
            formatted_date_str = f"{date_part} {time_part}"
            actual_date = datetime.strptime(formatted_date_str, "%Y.%m.%d %H:%M:%S")
            print(f"[날짜 확인] 게시글 실제 날짜: {actual_date}")
        except (ValueError, IndexError) as e:
            print(f"[날짜 파싱 오류] 날짜 문자열: {date_str}, 오류: {e}")

    # 추천수 추출
    recommend_elem = soup.find("span", class_="like_value")
    recommend = recommend_elem.text if recommend_elem else "0"

    # 게시글 내용 추출
    content_div = soup.find("div", class_="view_content")
    if not content_div:
        print(f"내용 영역을 찾을 수 없습니다: {post_url}")
        return {"text": "내용을 찾을 수 없습니다.", "images": [], "recommend": recommend, "actual_date": actual_date}

    text_content = content_div.get_text(separator="\n", strip=True)
    print(f"추출된 텍스트 (처음 100자): {text_content[:100]}")

    # 이미지 URL 추출
    image_urls = []
    for img in content_div.find_all("img"):
        src = img.get("src")
        if src:
            if not src.startswith("http") and not src.startswith("//"):
                continue
            image_urls.append(src)
    
    image_urls = ["https:" + url if url.startswith("//") else url for url in image_urls]
    print(f"추출된 이미지 URL: {len(image_urls)}개")

    time.sleep(delay)
    return {"text": text_content, "images": image_urls, "recommend": recommend, "actual_date": actual_date}

def clean_text(text):
    """텍스트 내의 불필요한 공백과 특수 문자를 제거합니다."""
    text = text.strip()
    text = html.unescape(text)  # HTML 엔티티 디코딩
    return text

def ruliweb_politics_crawl(url: str = 'https://bbs.ruliweb.com/community/board/300148',
                        delay: int = 5,
                        min_views: int = 1000,
                        max_consecutive_not_today=3):
    today = datetime.now().date()
    data = []
    
    # 중복 체크를 위한 집합
    post_ids_set = set()
    post_links_set = set()
    
    # 오늘 날짜가 아닌 게시물 연속 카운터
    consecutive_not_today_posts = 0
    
    # 페이지 번호 (1부터 시작)
    page_num = 1
    
    print(f"\n[크롤링 시작] 오늘 날짜: {today}, 최소 조회수: {min_views}")
    print(f"[설정] 연속 오늘 날짜 아닌 게시글 제한: {max_consecutive_not_today}개")
    
    while True:
        page_url = f"{url}?page={page_num}" if page_num > 1 else url
        print(f"\n[페이지 접근] 페이지 {page_num}: {page_url}")
        
        try:
            headers = get_headers()
            response = requests.get(page_url, headers=headers, timeout=10)
            response.encoding = 'utf-8'
            soup = Soup(response.text, "html.parser")
            print(f"[페이지 로드 성공] 상태 코드: {response.status_code}")
        except Exception as e:
            print(f"[페이지 로드 오류]: {str(e)}")
            break

        # 게시글 목록 찾기 (루리웹 정치게시판 구조)
        board_list = soup.find("table", class_="board_list_table")
        if not board_list:
            print("[게시판 데이터를 찾을 수 없습니다]")
            break
        
        all_posts = board_list.find_all("tr", class_="table_body")
        print(f"[게시글 목록] 발견된 총 게시글 수: {len(all_posts)}개")
        
        for post in all_posts:
            try:
                # 공지사항 제외
                if post.find("span", class_="notice"):
                    print("[공지사항 제외]")
                    continue
                
                # 조회수 확인
                hit_elem = post.find("td", class_="hit")
                if not hit_elem:
                    print("[조회수 요소 없음]")
                    continue
                
                views_text = hit_elem.text.strip().replace(',', '')
                if not views_text.isdigit():
                    print(f"[조회수 형식 오류] {views_text}")
                    continue
                
                views = int(views_text)
                if views < min_views:
                    print(f"[조회수 미달] {views} < {min_views}")
                    continue
                
                # 제목 및 링크 추출
                subject_elem = post.find("td", class_="subject")
                if not subject_elem or not subject_elem.find("a"):
                    print("[제목 요소 없음]")
                    continue
                
                title_a = subject_elem.find("a", class_="subject_link")
                if not title_a:
                    print("[제목 링크 요소 없음]")
                    continue
                    
                title = clean_text(title_a.text)
                link = title_a["href"]
                if not link.startswith("http"):
                    link = "https://bbs.ruliweb.com" + link
                
                print(f"[게시글 발견] 제목: {title}, 조회수: {views}")
                
                # 게시글 내용 및 실제 날짜 확인
                content_data = get_post_content(link, delay=delay)
                
                # 실제 날짜가 오늘인지 확인
                actual_date = content_data.get("actual_date")
                if not actual_date:
                    print(f"[날짜 확인 불가] 게시글: {title}")
                    consecutive_not_today_posts += 1
                    print(f"[연속 오늘 아닌 게시글] {consecutive_not_today_posts}/{max_consecutive_not_today}")
                    
                    if consecutive_not_today_posts >= max_consecutive_not_today:
                        print(f"[크롤링 종료] 연속 {max_consecutive_not_today}개의 오늘 날짜 아닌 게시글 발견")
                        break
                    continue
                
                if actual_date.date() != today:
                    print(f"[오늘 날짜 아님] 게시글 날짜: {actual_date.date()}, 오늘 날짜: {today}")
                    consecutive_not_today_posts += 1
                    print(f"[연속 오늘 아닌 게시글] {consecutive_not_today_posts}/{max_consecutive_not_today}")
                    
                    if consecutive_not_today_posts >= max_consecutive_not_today:
                        print(f"[크롤링 종료] 연속 {max_consecutive_not_today}개의 오늘 날짜 아닌 게시글 발견")
                        break
                    continue
                
                # 오늘 날짜 게시글 처리
                consecutive_not_today_posts = 0  # 오늘 날짜 게시글 발견시 카운터 초기화
                
                # 작성자 추출
                writer_elem = post.find("td", class_="name")
                writer = clean_text(writer_elem.text) if writer_elem else "N/A"
                
                # 카테고리 추출
                category_elem = post.find("td", class_="divsn")
                category = category_elem.text.strip() if category_elem else "정치"
                
                # 게시글 ID 추출
                post_id = link.split("/")[-1].split("?")[0]
                
                # 중복 체크
                if post_id in post_ids_set or link in post_links_set:
                    print(f"[중복된 게시글 건너뜀]: {title}")
                    continue
                
                post_ids_set.add(post_id)
                post_links_set.add(link)
                
                data.append({
                    "Post_ID": post_id,
                    "Community": "6p",
                    "Category": category,
                    "Title": title,
                    "Link": link,
                    "Writer": writer,
                    "Date": actual_date,
                    "Recommend": content_data["recommend"],
                    "Views": views,
                    "Content": content_data["text"],
                    "Images": content_data["images"]
                })
                print(f"[게시글 추가됨] 제목: {title}, 조회수: {views}, 날짜: {actual_date}")
                
                time.sleep(random.uniform(1, 3))
                
            except Exception as e:
                print(f"[데이터 추출 중 오류 발생]: {e}")
                continue
        
        # 연속 오늘 날짜 아닌 게시글 제한 초과 확인
        if consecutive_not_today_posts >= max_consecutive_not_today:
            print(f"[크롤링 종료] 연속 {max_consecutive_not_today}개의 오늘 날짜 아닌 게시글 발견")
            break
        
        page_num += 1
        time.sleep(random.uniform(2, 5))

    print(f"\n[크롤링 완료] 총 수집된 게시글: {len(data)}개")
    
    if data:
        df = pd.DataFrame(data)
        df = df.sort_values(by="Date", ascending=False)
        return df
    return None

if __name__ == "__main__":
    # 오늘 날짜 폴더 경로 설정
    base_data_folder = os.path.join('/code/data')
    today = datetime.now().strftime('%Y%m%d')
    today_folder = os.path.join(base_data_folder, today)
    
    # 오늘 날짜 폴더가 없으면 생성
    if not os.path.exists(today_folder):
        os.makedirs(today_folder, exist_ok=True)
        print(f"'{today_folder}' 폴더를 생성했습니다.")
    
    df = ruliweb_politics_crawl(
        delay=5, 
        min_views=1000, 
        max_consecutive_not_today=3  # 오늘 날짜가 아닌 게시글이 연속 3개 이상이면 종료
    )
    
    if df is not None and not df.empty:
        available_cols = [col for col in ["Post_ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Content", "Images"] if col in df.columns]
        print(df[available_cols])
        
        # 오늘 날짜 폴더에 CSV 파일 저장
        file_name = f"ruliweb_politics_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"[크롤링 완료] 데이터 저장 경로: {file_path}")
