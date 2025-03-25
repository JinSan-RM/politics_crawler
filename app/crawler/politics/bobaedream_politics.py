import pandas as pd
from datetime import datetime
import time
import requests
from bs4 import BeautifulSoup as Soup
import random
import os
import re

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
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://www.bobaedream.co.kr/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1"
    }

# 유효한 URL 확인
def is_valid_post_url(url):
    if not url or "javascript:" in url:
        return False
    return url.startswith("http")

# URL에서 게시글 ID 추출 함수
def extract_post_id(url):
    match = re.search(r'No=(\d+)', url)
    if match:
        return match.group(1)
    return None

# 게시글 내용 크롤링 (BeautifulSoup만 사용)
def get_post_content(post_url, delay=2):
    if not is_valid_post_url(post_url):
        print(f"유효하지 않은 URL 건너뜀: {post_url}")
        return {"text": "유효하지 않은 URL", "images": []}

    start_time = time.time()
    try:
        headers = get_headers()
        response = requests.get(post_url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = Soup(response.text, "html.parser")
        print(f"크롤링 중: {post_url}, 응답 시간: {time.time() - start_time:.2f}초")
    except Exception as e:
        print(f"게시글 페이지 로드 오류: {post_url} - {str(e)}, 소요 시간: {time.time() - start_time:.2f}초")
        return {"text": f"로드 오류: {str(e)}", "images": []}

    content_div = soup.find("div", class_="bodyCont")
    if not content_div:
        try:
            content_div = soup.find("div", class_="bbs_content")
            if not content_div:
                print(f"내용 영역을 찾을 수 없습니다: {post_url}")
                return {"text": "내용을 찾을 수 없습니다.", "images": []}
        except:
            print(f"내용 영역을 찾을 수 없습니다: {post_url}")
            return {"text": "내용을 찾을 수 없습니다.", "images": []}

    text_content = content_div.get_text(separator="\n", strip=True)
    print(f"추출된 텍스트 (처음 100자): {text_content[:100]}")
    image_urls = [img.get("src") for img in content_div.find_all("img") if img.get("src")]
    image_urls = ["https:" + url if url.startswith("//") else url for url in image_urls]
    print(f"추출된 이미지 URL: {len(image_urls)}개")

    time.sleep(delay)
    print(f"게시글 크롤링 완료: {post_url}, 총 소요 시간: {time.time() - start_time:.2f}초")
    return {"text": text_content, "images": image_urls}

# 보배드림 정치 게시판 크롤링 (오늘 날짜만, 최대 3페이지 뒤까지 확인)
def bobaedream_politic_crawl(url: str = 'https://www.bobaedream.co.kr/list?code=politic',
                             delay: int = 2,
                             min_views: int = 150,
                             max_pages_to_check=3):
    start_time = time.time()
    today = datetime.now().date()
    data = []
    page = 1
    today_posts_found = False
    pages_checked_after_no_today = 0

    print(f"\n[크롤링 시작] 오늘 날짜: {today}, 최소 조회수: {min_views}, 시작 시간: {datetime.fromtimestamp(start_time)}")
    print(f"[설정] 최대 확인 페이지 (오늘 날짜 없으면): {max_pages_to_check}페이지 뒤까지")

    while True:
        # 총 실행 시간 확인
        elapsed_time = time.time() - start_time
        if elapsed_time > 1100:  # 1100초(18분 20초) 초과 시 종료
            print(f"[타임아웃 방지] 총 실행 시간 {elapsed_time:.2f}초 초과, 크롤링 종료")
            break

        page_url = f"{url}&page={page}" if page > 1 else url
        print(f"\n[페이지 접근] 페이지 {page}: {page_url}, 경과 시간: {elapsed_time:.2f}초")

        page_start_time = time.time()
        try:
            headers = get_headers()
            response = requests.get(page_url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = 'utf-8'
            soup = Soup(response.text, "html.parser")
            print(f"[페이지 로드 성공] 상태 코드: {response.status_code}, 소요 시간: {time.time() - page_start_time:.2f}초")
        except Exception as e:
            print(f"[페이지 로드 오류]: {str(e)}, 소요 시간: {time.time() - page_start_time:.2f}초")
            break

        board = soup.find("table", id="boardlist")
        if not board:
            print("게시판 데이터를 찾을 수 없습니다.")
            break

        posts = []
        today_page_posts = False
        for post in board.find("tbody").find_all("tr"):
            # 공지사항 및 이벤트 제외
            if post.find("td", class_="c") or "best" in post.get("class", []):
                print("[공지사항 또는 베스트 게시글 제외]")
                continue

            # 게시글 ID
            post_id_elem = post.find("td", class_="num01")
            if not post_id_elem:
                continue
            post_id = post_id_elem.text.strip()

            # 제목 및 링크
            title_elem = post.find("a", class_="bsubject")
            if not title_elem:
                continue
            title = title_elem.text.strip()
            link = title_elem["href"]
            link = f"https://www.bobaedream.co.kr{link}" if not link.startswith('http') else link

            # 작성자
            writer_elem = post.find("span", class_="author")
            writer = writer_elem.text.strip() if writer_elem else "N/A"

            # 날짜
            date_elem = post.find("td", class_="date")
            date_str = date_elem.text.strip() if date_elem else ""
            post_date = None
            if ":" in date_str:  # HH:MM 형식이면 오늘 날짜로 간주
                try:
                    post_date = datetime.strptime(f"{today.strftime('%Y-%m-%d')} {date_str}", "%Y-%m-%d %H:%M")
                    today_page_posts = True
                    today_posts_found = True
                except ValueError:
                    print(f"날짜 파싱 오류: {date_str}")
                    continue
            else:
                # HH:MM 형식이 아니면 다른 날짜로 간주
                print(f"[오늘 날짜 아님] 게시글 날짜: {date_str}")
                continue

            # 추천수
            recommend_elem = post.find("td", class_="recomm")
            recommend = "0"
            if recommend_elem:
                font_elem = recommend_elem.find('font')
                if font_elem:
                    recommend = font_elem.text.strip()

            # 조회수
            views_elem = post.find("td", class_="count")
            views = int(views_elem.text.strip().replace(',', '')) if views_elem and views_elem.text.strip().replace(',', '').isdigit() else 0

            # 최소 조회수 이상인 경우에만 추가
            if views >= min_views:
                posts.append({
                    "Post ID": post_id,
                    "Community": "7p",
                    "Category": "정치",
                    "Title": title,
                    "Link": link,
                    "Writer": writer,
                    "Date": post_date,
                    "Recommend": recommend,
                    "Views": views
                })
                print(f"[게시글 발견] 제목: {title}, 조회수: {views}, 날짜: {post_date}")

        # 오늘 날짜 게시글 여부 확인
        if not today_page_posts:
            pages_checked_after_no_today += 1
            print(f"[오늘 날짜 게시글 없음] 페이지 {page}, 확인한 페이지 수: {pages_checked_after_no_today}/{max_pages_to_check}")
            if pages_checked_after_no_today >= max_pages_to_check:
                print(f"[크롤링 종료] 오늘 날짜 게시글 없음, {max_pages_to_check}페이지 뒤까지 확인 완료")
                break
        else:
            pages_checked_after_no_today = 0  # 오늘 날짜 게시글 발견 시 카운터 초기화

        # 게시글 내용 크롤링
        for post in posts:
            post_start_time = time.time()
            content_data = get_post_content(post["Link"], delay=delay)
            if content_data["text"] not in ["내용을 찾을 수 없습니다.", "유효하지 않은 URL"] and not content_data["text"].startswith("로드 오류"):
                post["Content"] = content_data["text"]
                post["Images"] = content_data["images"]
                data.append(post)
                print(f"[게시글 추가됨] 제목: {post['Title']}, 조회수: {post['Views']}, 소요 시간: {time.time() - post_start_time:.2f}초")
            else:
                print(f"게시글 내용 추출 실패, 제외됨: {post['Link']}")
            time.sleep(random.uniform(1, 2))

        print(f"[페이지 완료] 페이지 {page} 처리 완료, 총 경과 시간: {time.time() - start_time:.2f}초")
        page += 1
        time.sleep(random.uniform(2, 4))

    print(f"\n[크롤링 완료] 총 수집된 게시글: {len(data)}개, 총 소요 시간: {time.time() - start_time:.2f}초")
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
        try:
            os.makedirs(today_folder, exist_ok=True)
            print(f"'{today_folder}' 폴더를 생성했습니다.")
        except Exception as e:
            print(f"폴더 생성 중 오류 발생: {e}")

    df = bobaedream_politic_crawl(
        delay=2,
        min_views=50,
        max_pages_to_check=3
    )
    if df is not None and not df.empty:
        print(df[["Post ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Content", "Images"]])
        # 오늘 날짜 폴더에 CSV 파일 저장
        file_name = f"bobaedream_politics_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 '{file_path}' 파일로 저장되었습니다.")
    else:
        print("수집된 데이터가 없습니다.")