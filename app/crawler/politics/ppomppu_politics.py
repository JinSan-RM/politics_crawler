import pandas as pd
from datetime import datetime
import time
import requests
from bs4 import BeautifulSoup as Soup
import random
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
        "Referer": "https://www.ppomppu.co.kr/zboard/zboard.php?id=freeboard"
    }

# 유효한 URL 확인
def is_valid_post_url(url):
    if not url or "javascript:" in url:
        return False
    return url.startswith("http")

# 게시글 내용 크롤링 (텍스트와 이미지 경로만 추출)
def get_post_content(post_url, delay=2):
    if not is_valid_post_url(post_url):
        print(f"유효하지 않은 URL 건너뜀: {post_url}", flush=True)
        return {"text": "유효하지 않은 URL", "images": []}

    try:
        headers = get_headers()
        response = requests.get(post_url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"페이지 로드 실패: {post_url} - 상태 코드: {response.status_code}", flush=True)
            print(f"응답 HTML (처음 1000자): {response.text[:1000]}", flush=True)
            return {"text": f"페이지 로드 실패: 상태 코드 {response.status_code}", "images": []}

        response.encoding = 'euc-kr'
        soup = Soup(response.text, "html.parser")

        # <table class="pic_bg">를 찾음
        pic_bg_tables = soup.find_all("table", class_="pic_bg")
        if not pic_bg_tables:
            print(f"pic_bg 테이블을 찾을 수 없음: {post_url}", flush=True)
            print(f"응답 HTML (처음 1000자): {response.text[:1000]}", flush=True)
            return {"text": "pic_bg 테이블을 찾을 수 없습니다.", "images": []}

        # 본문 텍스트와 이미지 경로 추출
        text_parts = []
        image_urls = []

        for pic_bg_table in pic_bg_tables:
            # <p> 태그에서 텍스트 추출
            for p in pic_bg_table.find_all("p"):
                p_text = p.get_text(strip=True)
                # 공백( )이나 빈 문자열 제외
                if p_text and p_text != '\xa0':
                    text_parts.append(p_text)

            # <img> 태그에서 이미지 경로 추출
            for img in pic_bg_table.find_all("img"):
                src = img.get("src")
                if src:
                    # //로 시작하는 URL은 https: 추가
                    if src.startswith("//"):
                        src = "https:" + src
                    image_urls.append(src)

        text_content = "\n".join(text_parts) if text_parts else "텍스트 없음"
        print(f"추출된 텍스트: {text_content}", flush=True)
        print(f"추출된 이미지 경로: {image_urls}", flush=True)

        time.sleep(delay)
        return {"text": text_content, "images": image_urls}

    except Exception as e:
        print(f"게시글 크롤링 오류: {post_url} - {str(e)}", flush=True)
        return {"text": f"오류 발생: {str(e)}", "images": []}

# 추천 수 파싱 함수 추가
def parse_recommend(recommend_str):
    """
    '3 - 0'과 같은 형식에서 추천 수만 추출하여 정수로 반환합니다.
    """
    try:
        if not recommend_str or recommend_str == '':
            return 0
        if ' - ' in recommend_str:
            recommend, _ = recommend_str.split(' - ')
            return int(recommend.strip())
        return int(recommend_str.strip())  # 숫자만 있는 경우
    except (ValueError, AttributeError) as e:
        print(f"추천 수 파싱 오류: {recommend_str} - {str(e)}", flush=True)
        return 0  # 기본값

# 게시판 크롤링 (오늘 날짜만, 최대 페이지 제한 추가)
def ppomppu_freeboard_crawl(url='https://www.ppomppu.co.kr/zboard/zboard.php?id=issue',
                            delay=2, min_views=300, max_pages=10):
    today = datetime.now().date()
    data = []
    page = 1

    while page <= max_pages:
        page_url = f"{url}&page={page}" if page > 1 else url
        try:
            headers = get_headers()
            response = requests.get(page_url, headers=headers, timeout=10)
            if response.status_code != 200:
                print(f"목록 페이지 로드 실패: {page_url} - 상태 코드: {response.status_code}", flush=True)
                break
            response.encoding = 'euc-kr'
            soup = Soup(response.text, "html.parser")
            print(f"목록 페이지 로드 완료: {page_url}", flush=True)
        except Exception as e:
            print(f"목록 페이지 로드 오류: {str(e)}", flush=True)
            break

        board = soup.find("table", id="revolution_main_table")
        if not board:
            print("게시판 테이블을 찾을 수 없습니다.", flush=True)
            break

        posts = []
        found_today = False
        # 공지사항과 알림 제외: class="baseList"이면서 공지/알림이 아닌 게시글만 처리
        for post in board.find_all("tr", class_="baseList"):
            post_num_elem = post.find("td", class_="baseList-numb")
            if not post_num_elem or post_num_elem.text.strip() in ["공지", "알림"]:
                continue
            post_num = post_num_elem.text.strip()

            # 제목과 링크
            title_elem = post.find("a", class_="baseList-title")
            if not title_elem:
                continue
            title = title_elem.text.strip()
            href = title_elem["href"]
            link = "https://www.ppomppu.co.kr/zboard/" + href if href.startswith("view.php") else href

            # 작성자
            writer_elem = post.find("a", class_="baseList-name")
            writer = writer_elem.text.strip() if writer_elem else "N/A"

            # 날짜 (title 속성에서 추출)
            date_elem = post.find("td", class_="baseList-space", title=True)
            if not date_elem:
                print(f"날짜 요소를 찾을 수 없음: Post ID {post_num}", flush=True)
                continue
            date_str = date_elem["title"].strip()  # title 속성에서 전체 날짜 추출 (예: "25.03.20 06:02:42")
            print(f"추출된 날짜 (title): {date_str}", flush=True)

            try:
                # title 속성에서 날짜 파싱 (형식: YY.MM.DD HH:MM:SS)
                post_date = datetime.strptime(date_str, "%y.%m.%d %H:%M:%S")
                print(f"파싱된 날짜: {post_date}", flush=True)
                if post_date.date() != today:
                    print(f"오늘 날짜 아님: {post_date.date()} (오늘: {today})", flush=True)
                    continue
                found_today = True
            except ValueError as e:
                print(f"날짜 파싱 오류: {date_str} - {str(e)}", flush=True)
                continue

            # 추천 수 (추천 수만 추출)
            recommend_elem = post.find("td", class_="baseList-rec")
            recommend_str = recommend_elem.text.strip() if recommend_elem else "0"
            recommend = parse_recommend(recommend_str)  # 추천 수만 파싱
            print(f"추출된 추천 수: {recommend}", flush=True)

            # 조회수
            views_elem = post.find("td", class_="baseList-views")
            views = int(views_elem.text.strip()) if views_elem and views_elem.text.strip().isdigit() else 0
            print(f"조회수: {views}", flush=True)

            # 최소 조회수 조건
            if views >= min_views:
                category_elem = post.find("span", class_="baseList-category")
                category = category_elem.text.strip() if category_elem else "N/A"

                posts.append({
                    "Post ID": post_num,
                    "Community": "5p",
                    "Category": category,
                    "Title": title,
                    "Link": link,
                    "Writer": writer,
                    "Date": post_date,
                    "Recommend": str(recommend),  # 문자열로 변환
                    "Views": str(views)  # 문자열로 변환
                })
                print(f"게시글 추가됨: {title} (조회수: {views}, 추천 수: {recommend})", flush=True)

        if not found_today:
            print(f"페이지 {page}에서 오늘 날짜 게시글을 찾을 수 없습니다. 크롤링 종료.", flush=True)
            break

        if not posts:
            print(f"페이지 {page}에서 조건에 맞는 게시글 없음. 다음 페이지로 이동.", flush=True)
            page += 1
            time.sleep(random.uniform(1, 3))
            continue

        for post in posts:
            content_data = get_post_content(post["Link"], delay=delay)
            if content_data["text"] not in ["pic_bg 테이블을 찾을 수 없습니다.", "유효하지 않은 URL"] and not content_data["text"].startswith("오류 발생"):
                post["Content"] = content_data["text"]
                post["Images"] = content_data["images"]
                data.append(post)
            else:
                print(f"게시글 내용 추출 실패, 제외됨: {post['Link']}", flush=True)
            time.sleep(random.uniform(1, 3))

        print(f"페이지 {page} 크롤링 완료. 다음 페이지로 이동.", flush=True)
        page += 1
        time.sleep(random.uniform(1, 3))

    if page > max_pages:
        print(f"최대 페이지 수({max_pages})에 도달하여 크롤링 종료.", flush=True)

    if data:
        df = pd.DataFrame(data)
        df = df.sort_values(by="Date", ascending=False)
        return df
    return None

if __name__ == "__main__":
    base_data_folder = os.path.join('/code/data')  # Docker 환경
    today = datetime.now().strftime('%Y%m%d')
    today_folder = os.path.join(base_data_folder, today)

    if not os.path.exists(today_folder):
        try:
            os.makedirs(today_folder, exist_ok=True)
            print(f"'{today_folder}' 폴더를 생성했습니다.", flush=True)
        except Exception as e:
            print(f"폴더 생성 중 오류 발생: {e}", flush=True)

    df = ppomppu_freeboard_crawl(
        delay=2,
        min_views=150,
        max_pages=10  # 최대 10페이지까지만 크롤링
    )
    if df is not None:
        available_cols = [col for col in ["Post ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Content", "Images"] if col in df.columns]
        print(df[available_cols], flush=True)
        file_name = f"ppomppu_politics_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 '{file_path}' 파일로 저장되었습니다.", flush=True)
    else:
        print("크롤링된 데이터가 없습니다.", flush=True)