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
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...",
        "Mozilla/5.0 (X11; Linux x86_64)..."
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
        "Connection": "keep-alive",
        "Referer": "https://www.bobaedream.co.kr/"
    }

# URL에서 게시글 ID 추출 함수
def extract_post_id(url):
    match = re.search(r'No=(\d+)', url)
    if match:
        return match.group(1)
    return None

# 게시글 내용 크롤링 (정적 방식)
def get_post_content(post_url):
    try:
        headers = get_headers()
        response = requests.get(post_url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = 'utf-8'  # 이 부분 추가 필수!
        soup = Soup(response.text, "html.parser")

        # 게시글 ID 추출 시도 (copyAddress 클래스에서)
        post_id = None
        copy_address = soup.find("p", class_="copyAddress")
        if copy_address:
            button = copy_address.find("button", class_="ipAdd")
            if button:
                url_text = button.get_text(strip=True)
                post_id = extract_post_id(url_text)

        content_div = soup.find("div", class_="bodyCont") or soup.find("div", id="bodyCont")
        if not content_div:
            print(f"내용 영역을 찾을 수 없습니다: {post_url}")
            return {"text": "", "images": [], "post_id": post_id}

        text_content = content_div.get_text(separator="\n", strip=True)

        image_urls = []
        for img in content_div.find_all("img"):
            src = img.get("src")
            if src:
                if src.startswith("//"):
                    src = f"https:{src}"
                image_urls.append(src)

        return {"text": text_content, "images": image_urls, "post_id": post_id}
    except Exception as e:
        print(f"게시글 크롤링 실패: {post_url} - {e}")
        return {"text": "", "images": [], "post_id": None}

# 보배드림 베스트 게시판 크롤링 메인 함수 (정적 방식)
def bobaedream_bestboard_crawl(min_views=10000, max_page=3):
    base_url = 'https://www.bobaedream.co.kr/list?code=best'
    today = datetime.now().date()
    data = []

    for page in range(1, max_page + 1):
        page_url = f"{base_url}&page={page}"
        print(f"페이지 {page} 크롤링 중: {page_url}")

        try:
            headers = get_headers()
            response = requests.get(page_url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = 'utf-8'  # 이 부분 추가 필수!
            soup = Soup(response.text, "html.parser")
            board_table = soup.find("table", id="boardlist")
            if not board_table:
                print("게시판 테이블을 찾을 수 없습니다.")
                continue

            posts = board_table.find("tbody").find_all("tr", attrs={"itemtype": "http://schema.org/Article"})
            for post in posts:
                if post.get("class") and "notice" in post.get("class"):
                    continue

                date_str = post.find("td", class_="date").text.strip()
                if ":" not in date_str:  # 오늘 날짜 아닌 경우 skip
                    continue

                views_text = post.find("td", class_="count").text.strip().replace(',', '')
                views = int(views_text) if views_text.isdigit() else 0
                if views < min_views:
                    continue

                title_elem = post.find("a", class_="bsubject")
                title = title_elem.text.strip()
                link = title_elem["href"]
                link = f"https://www.bobaedream.co.kr{link}" if not link.startswith('http') else link
                
                # URL에서 게시글 ID 추출
                post_id = extract_post_id(link)

                category_elem = post.find("td", class_="category")
                category = category_elem.text.strip() if category_elem else ""

                writer_elem = post.find("span", class_="author")
                writer = writer_elem.text.strip() if writer_elem else ""

                recommend_elem = post.find("td", class_="recomm").find("font")
                recommend_text = recommend_elem.text.strip() if recommend_elem else '0'
                recommend = int(recommend_text) if recommend_text.isdigit() else 0

                content_data = get_post_content(link)
                
                # 게시글 상세 페이지에서 추출한 ID가 있으면 그것을 사용, 없으면 URL에서 추출한 ID 사용
                final_post_id = content_data.get("post_id") or post_id

                data.append({
                    "Post ID": final_post_id,  # 게시글 ID 추가
                    "Community": "7",
                    "Category": category,
                    "Title": title,
                    "Link": link,
                    "Writer": writer,
                    "Date": f"{today} {date_str}",
                    "Recommend": recommend,
                    "Views": views,
                    "Content": content_data["text"],
                    "Images": content_data["images"]
                })

                print(f"게시물 수집 완료: {title} (ID: {final_post_id})")

            time.sleep(random.uniform(2, 5))

        except Exception as e:
            print(f"페이지 로드 오류: {e}")
            continue

    df = pd.DataFrame(data)
    return df if not df.empty else None

# 메인 실행부
if __name__ == "__main__":
    base_data_folder = os.path.join('/code/data')  # Docker 경로로 수정
    today = datetime.now().strftime('%Y%m%d')
    today_folder = os.path.join(base_data_folder, today)
    
    # 오늘 날짜 폴더가 없으면 생성
    if not os.path.exists(today_folder):
        try:
            os.makedirs(today_folder, exist_ok=True)
            print(f"'{today_folder}' 폴더를 생성했습니다.")
        except Exception as e:
            print(f"폴더 생성 중 오류 발생: {e}")
    
    df = bobaedream_bestboard_crawl(min_views=7000)  # 최소 조회수 10000으로 설정
    if df is not None:
        available_cols = [col for col in ["Post_ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Content", "Images"] if col in df.columns]
        print(df[available_cols])
        
        # CSV 파일 저장 시 utf-8-sig로 인코딩 설정 (한글 깨짐 방지)
        file_name = f"bobaedream_bestboard_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 '{file_path}' 파일로 저장되었습니다.")
