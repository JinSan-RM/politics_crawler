import pandas as pd
from datetime import datetime
import time
import requests
from bs4 import BeautifulSoup as Soup
import random
import re
import os

# 요청 헤더 설정 (크롤러 차단 방지)
def get_headers():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.fmkorea.com/",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
    }

# 텍스트 필터링 (한글, 영어, 기본 기호만 남김)
def filter_korean_english(text):
    if not text:
        return ""
    return re.sub(r'[^\w\s가-힣a-zA-Z.,!?]', '', text)

# 유효한 게시글 URL인지 확인
def is_valid_post_url(url):
    if not url or "javascript:" in url:
        return False
    return url.startswith("http")

# 페이지 존재 여부 확인
def check_page_exists(url, headers):
    try:
        response = requests.get(url, headers=headers, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"페이지 접근 오류: {e}")
        return False

# 개별 게시글 크롤링
def get_post_content(post_url, max_retries=2, timeout=10, delay=2):
    if not is_valid_post_url(post_url):
        print(f"유효하지 않은 URL 건너뜀: {post_url}")
        return {"text": "유효하지 않은 URL", "images": []}

    attempt = 0
    start_time = time.time()
    time.sleep(random.uniform(3, 5))  # 초기 지연 시간 조정

    while attempt < max_retries:
        try:
            headers = get_headers()
            response = requests.get(post_url, headers=headers, timeout=timeout)
            response.raise_for_status()
            response.encoding = 'utf-8'
            print(f"크롤링 중: {post_url}, 응답 시간: {time.time() - start_time:.2f}초")

            soup = Soup(response.text, "html.parser")
            content_div = soup.find("div", class_="xe_content")

            if not content_div:
                print(f"내용 영역을 찾을 수 없습니다: {post_url}")
                return {"text": "내용을 찾을 수 없음", "images": []}

            text_content = content_div.get_text(separator="\n", strip=True)
            filtered_text = filter_korean_english(text_content)

            image_urls = []
            for img in content_div.find_all("img"):
                if img.get("src"):
                    src = img.get("src")
                    if not src.startswith("http"):
                        src = "https://www.fmkorea.com" + src
                    image_urls.append(src)

            print(f"게시글 내용 추출 완료: {len(filtered_text)}자, 이미지 {len(image_urls)}개")
            time.sleep(delay)
            print(f"게시글 크롤링 완료: {post_url}, 총 소요 시간: {time.time() - start_time:.2f}초")
            return {"text": filtered_text, "images": image_urls}

        except Exception as e:
            attempt += 1
            if attempt == max_retries:
                print(f"게시글 로드 실패: {str(e)}, 소요 시간: {time.time() - start_time:.2f}초")
                return {"text": f"로드 실패: {str(e)}", "images": []}
            time.sleep(random.uniform(2, 4))

# 게시판 크롤링 함수 (오늘 날짜 게시글만 수집)
def fmkorea_politics_crawl(min_views=100, max_pages=10, max_consecutive_empty=3):
    start_time = time.time()
    base_url = 'https://www.fmkorea.com/politics'
    data = []
    page = 1
    today = datetime.now().date()

    # 중복 체크를 위한 집합
    processed_links = set()
    consecutive_empty_pages = 0

    print(f"\n[크롤링 시작] 오늘 날짜: {today}, 최소 조회수: {min_views}, 최대 페이지: {max_pages}, 시작 시간: {datetime.fromtimestamp(start_time)}")
    print(f"[설정] 연속 오늘 날짜 아닌 페이지 제한: {max_consecutive_empty}페이지")

    while page <= max_pages:
        # 총 실행 시간 확인
        elapsed_time = time.time() - start_time
        if elapsed_time > 1100:  # 1100초(18분 20초) 초과 시 종료
            print(f"[타임아웃 방지] 총 실행 시간 {elapsed_time:.2f}초 초과, 크롤링 종료")
            break

        # 페이지 URL 설정
        if page == 1:
            page_url = base_url
        else:
            page_url = f"https://www.fmkorea.com/index.php?mid=politics&page={page}"

        print(f"\n[페이지 접근] 페이지 {page}: {page_url}, 경과 시간: {elapsed_time:.2f}초")

        page_start_time = time.time()
        max_retries = 2
        attempt = 0
        while attempt < max_retries:
            try:
                headers = get_headers()
                response = requests.get(page_url, headers=headers, timeout=10)
                response.raise_for_status()
                response.encoding = 'utf-8'
                print(f"[페이지 로드 성공] 상태 코드: {response.status_code}, 소요 시간: {time.time() - page_start_time:.2f}초")
                break
            except Exception as e:
                attempt += 1
                if attempt == max_retries:
                    print(f"[페이지 로드 오류 (최종)]: {str(e)}, 소요 시간: {time.time() - page_start_time:.2f}초")
                    page += 1
                    time.sleep(random.uniform(3, 5))
                    continue
                print(f"[페이지 로드 오류 (재시도 {attempt}/{max_retries})]: {str(e)}, 소요 시간: {time.time() - page_start_time:.2f}초")
                time.sleep(random.uniform(3, 5))

        if attempt == max_retries:
            continue

        soup = Soup(response.text, "html.parser")
        tbody = soup.find("tbody")
        if not tbody:
            print(f"페이지 {page}에서 게시글 목록을 찾을 수 없습니다.")
            page += 1
            time.sleep(random.uniform(3, 5))
            continue

        # 게시글 목록 긁어오기
        posts = []
        today_posts_found = False
        skipped_posts = 0

        for post in tbody.find_all("tr"):
            if "notice" in post.get("class", []):
                print("[공지사항 제외]")
                continue

            # 날짜 확인
            date_elem = post.find("td", class_="time")
            if not date_elem:
                print("[날짜 요소 없음]")
                continue

            date_str = date_elem.text.strip()
            post_date = None
            try:
                if ":" in date_str:  # 오늘 날짜
                    post_date = datetime.strptime(f"{today} {date_str}", "%Y-%m-%d %H:%M")
                    today_posts_found = True
                else:
                    skipped_posts += 1
                    print(f"[오늘 날짜 아님] 게시글 날짜: {date_str}")
                    continue
            except ValueError as e:
                print(f"[날짜 파싱 오류] 날짜 문자열: {date_str}, 오류: {e}")
                continue

            # 조회수 확인
            views_elem = post.find("td", class_="m_no")
            if not views_elem or not views_elem.text.strip().isdigit():
                print("[조회수 요소 없음 또는 형식 오류]")
                continue

            views = int(views_elem.text.strip())
            if views < min_views:
                skipped_posts += 1
                print(f"[조회수 미달] {views} < {min_views}")
                continue

            # 제목과 링크 추출
            title_elem = post.find("td", class_="title")
            if not title_elem or not title_elem.find("a"):
                print("[제목 요소 없음]")
                continue

            link_elem = title_elem.find("a")
            link = link_elem.get("href", "")
            if not link:
                print("[링크 없음]")
                continue

            if not link.startswith("http"):
                link = "https://www.fmkorea.com" + link

            if link in processed_links:
                print(f"[중복된 게시글 건너뜀]: {link}")
                continue

            post_num = link.split("/")[-1]
            title = filter_korean_english(link_elem.text.strip())

            # 카테고리 추출
            cate_elem = post.find("td", class_="cate")
            category = ""
            if cate_elem and cate_elem.find("a"):
                category = filter_korean_english(cate_elem.find("a").text.strip())

            # 작성자 추출
            writer_elem = post.find("td", class_="author")
            writer = filter_korean_english(writer_elem.text.strip()) if writer_elem else ""

            # 추천수 추출
            recommend_elem = post.find_all("td", class_="m_no")
            recommend = 0
            if len(recommend_elem) > 1:
                recommend_text = recommend_elem[-1].text.strip()
                if recommend_text.isdigit():
                    recommend = int(recommend_text)

            print(f"[게시글 발견] 제목: {title}, 조회수: {views}, 날짜: {post_date}")

            posts.append({
                "Post ID": post_num,
                "Community": "11p",
                "Category": category,
                "Title": title,
                "Link": link,
                "Writer": writer,
                "Date": post_date,
                "Views": views,
                "Recommend": recommend
            })

        print(f"[게시글 목록] 페이지 {page}에서 발견된 총 게시글: {len(tbody.find_all('tr'))}개")
        print(f"[건너뛴 게시글] 오늘 날짜 아니거나 조회수 부족: {skipped_posts}개")
        print(f"[조건 충족] 게시글: {len(posts)}개")

        if not today_posts_found or len(posts) == 0:
            consecutive_empty_pages += 1
            print(f"[오늘 날짜 게시글 없음] 페이지 {page}, 연속 카운터: {consecutive_empty_pages}/{max_consecutive_empty}")
            if consecutive_empty_pages >= max_consecutive_empty:
                print(f"[크롤링 종료] 연속 {max_consecutive_empty}페이지 동안 오늘 날짜 게시글을 찾을 수 없음")
                break
        else:
            consecutive_empty_pages = 0
            print(f"[오늘 날짜 게시글 발견] 페이지 {page}, 연속 카운터 초기화")

        # 수집된 게시글 내용 가져오기
        for post in posts:
            post_start_time = time.time()
            content_data = get_post_content(post["Link"])
            if content_data["text"] not in ["내용을 찾을 수 없음", "유효하지 않은 URL"] and not content_data["text"].startswith("로드 실패"):
                post["Content"] = content_data["text"]
                post["Images"] = content_data["images"]
                data.append(post)
                processed_links.add(post["Link"])
                print(f"[게시글 추가됨] 제목: {post['Title']}, 조회수: {post['Views']}, 소요 시간: {time.time() - post_start_time:.2f}초")
            else:
                print(f"[게시글 내용 추출 실패, 제외됨]: {post['Link']}")
            time.sleep(random.uniform(3, 5))

        page += 1
        print(f"[페이지 완료] 페이지 {page-1} 완료, 총 경과 시간: {time.time() - start_time:.2f}초")
        time.sleep(random.uniform(3, 5))

    print(f"\n[크롤링 완료] 총 수집된 게시글: {len(data)}개, 총 소요 시간: {time.time() - start_time:.2f}초")

    if data:
        df = pd.DataFrame(data)
        df = df.sort_values(by="Date", ascending=False)
        return df
    return None

if __name__ == "__main__":
    base_data_folder = os.path.join('/code/data')
    today = datetime.now().strftime('%Y%m%d')
    today_folder = os.path.join(base_data_folder, today)

    if not os.path.exists(today_folder):
        os.makedirs(today_folder, exist_ok=True)
        print(f"'{today_folder}' 폴더를 생성했습니다.")

    df = fmkorea_politics_crawl(min_views=100, max_pages=10, max_consecutive_empty=3)
    if df is not None and not df.empty:
        available_cols = [col for col in ["Post ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Content", "Images"] if col in df.columns]
        print(df[available_cols])
        file_name = f"fmkorea_politics_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 '{file_path}' 파일로 저장되었습니다.")
    else:
        print("크롤링할 데이터가 없습니다.")