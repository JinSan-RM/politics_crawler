import requests
from bs4 import BeautifulSoup as Soup
import pandas as pd
from datetime import datetime
import time
import random
import os
import re

# 헤더 설정 함수
def get_headers():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
        "Connection": "keep-alive",
        "Referer": "https://www.clien.net/"
    }

# URL에서 게시글 ID 추출 함수
def extract_post_id(url):
    match = re.search(r'/(\d+)', url)
    if match:
        return match.group(1)
    return None

# 조회수 변환 함수
def parse_views(views_text):
    """조회수를 문자열에서 숫자로 변환 ('k' 단위 포함)"""
    views_text = views_text.strip().replace(',', '')
    if 'k' in views_text.lower():
        try:
            views_num = float(views_text.lower().replace('k', '')) * 1000
            return int(views_num)
        except ValueError:
            return 0
    else:
        try:
            return int(views_text)
        except ValueError:
            return 0

# 게시글 내용 크롤링 (정적 방식)
def get_post_content(post_url):
    try:
        headers = get_headers()
        response = requests.get(post_url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = Soup(response.text, "html.parser")

        content_div = soup.find("div", class_="post_article")
        if not content_div:
            print(f"내용 영역을 찾을 수 없습니다: {post_url}")
            return {"text": "", "images": []}

        text_content = content_div.get_text(separator="\n", strip=True)

        image_urls = []
        for img in content_div.find_all("img", class_="fr-dib"):
            src = img.get("src")
            if src:
                if src.startswith("//"):
                    src = f"https:{src}"
                elif not src.startswith("http"):
                    src = f"https://www.clien.net{src}"
                image_urls.append(src)

        return {"text": text_content, "images": image_urls}
    except Exception as e:
        print(f"게시글 크롤링 실패: {post_url} - {e}")
        return {"text": "", "images": []}

# 클리앙 게시판 크롤링 메인 함수
def clien_park_crawl(min_views=2000):
    base_url = 'https://www.clien.net/service/board/park'
    today = datetime.now().date()
    data = []
    page = 0  # 클리앙은 페이지 0부터 시작
    no_today_count = 0  # 오늘 날짜가 없는 페이지 연속 카운트
    max_no_today = 3    # 오늘 날짜 없는 페이지가 3번 연속이면 종료

    while no_today_count < max_no_today:
        page_url = base_url if page == 0 else f"{base_url}?&od=T31&category=0&po={page}"
        print(f"페이지 {page + 1} 크롤링 중: {page_url}")

        try:
            headers = get_headers()
            response = requests.get(page_url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = 'utf-8'
            soup = Soup(response.text, "html.parser")
            list_content = soup.find("div", class_="list_content")
            if not list_content:
                print("list_content를 찾을 수 없습니다.")
                no_today_count += 1
                page += 1
                continue

            posts = list_content.find_all("div", class_="list_item")
            if not posts:
                print("게시물이 없습니다.")
                no_today_count += 1
                page += 1
                continue

            has_today_post = False  # 페이지에 오늘 날짜 게시글 존재 여부
            for post in posts:
                post_id = post.get("data-board-sn")
                if not post_id:
                    continue

                title_elem = post.find("span", class_="subject_fixed")
                title = title_elem["title"].strip() if title_elem else ""
                link_elem = post.find("a", class_="list_subject")
                link = f"https://www.clien.net{link_elem['href']}" if link_elem else ""

                writer_elem = post.find("span", class_="nickname")
                writer = writer_elem.text.strip() if writer_elem else ""

                views_elem = post.find("div", class_="list_hit")
                views_text = views_elem.find("span", class_="hit").text.strip() if views_elem else "0"
                views = parse_views(views_text)
                if views < min_views:
                    continue

                time_elem = post.find("div", class_="list_time")
                time_str = time_elem.find("span", class_="time").text.strip() if time_elem else ""
                timestamp_elem = time_elem.find("span", class_="timestamp") if time_elem else None
                date_str = timestamp_elem.text.strip() if timestamp_elem else f"{today} {time_str}"

                # 날짜가 오늘인지 확인
                post_date = date_str.split()[0] if " " in date_str else date_str
                if post_date == str(today):
                    has_today_post = True

                like_elem = post.find("div", class_="list_symph")
                like_text = like_elem.find("span").text.strip() if like_elem else "0"
                recommend = int(like_text) if like_text.isdigit() else 0

                comment_elem = post.find("span", class_="rSymph05")
                comment_count = int(comment_elem.text.strip()) if comment_elem else 0

                content_data = get_post_content(link)

                data.append({
                    "Post ID": post_id,
                    "Community": "4",
                    "Category": "park",
                    "Title": title,
                    "Link": link,
                    "Writer": writer,
                    "Date": date_str,
                    "Recommend": recommend,
                    "Views": views,
                    "Comments": comment_count,
                    "Content": content_data["text"],
                    "Images": content_data["images"]
                })

                print(f"게시물 수집 완료: {title} (ID: {post_id}, Views: {views})")

            # 오늘 날짜 게시글이 없으면 카운트 증가
            if not has_today_post:
                no_today_count += 1
                print(f"오늘 날짜 게시글 없음. 연속 카운트: {no_today_count}/{max_no_today}")
            else:
                no_today_count = 0  # 오늘 날짜 게시글이 있으면 카운트 리셋

            page += 1
            time.sleep(random.uniform(2, 5))

        except Exception as e:
            print(f"페이지 로드 오류: {e}")
            no_today_count += 1
            page += 1
            continue

    print(f"오늘 날짜 게시글이 없는 페이지가 {max_no_today}번 연속으로 나와 크롤링을 종료합니다.")
    df = pd.DataFrame(data)
    return df if not df.empty else None

# 메인 실행부
if __name__ == "__main__":
    base_data_folder = os.path.join('/code/data')
    today = datetime.now().strftime('%Y%m%d')
    today_folder = os.path.join(base_data_folder, today)
    
    if not os.path.exists(today_folder):
        try:
            os.makedirs(today_folder, exist_ok=True)
            print(f"'{today_folder}' 폴더를 생성했습니다.")
        except Exception as e:
            print(f"폴더 생성 중 오류 발생: {e}")
    
    df = clien_park_crawl(min_views=2500)
    if df is not None:
        available_cols = [col for col in ["Post_ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Comments", "Content", "Images"] if col in df.columns]
        print(df[available_cols])
        
        file_name = f"clien_parkboard_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 '{file_path}' 파일로 저장되었습니다.")