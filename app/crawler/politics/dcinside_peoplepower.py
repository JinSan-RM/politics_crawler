import pandas as pd
from datetime import datetime
import time
import requests
from bs4 import BeautifulSoup as Soup
import random
from urllib.parse import urljoin
import os

# 헤더 설정 (User-Agent 회전)
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
        "Upgrade-Insecure-Requests": "1"
    }

# 유효한 게시글 URL인지 확인
def is_valid_post_url(url):
    if not url or "javascript:" in url or "addc.dcinside.com" in url:
        return False
    return url.startswith("http")

# 게시글 내용 및 이미지 크롤링
def get_post_content(post_url, delay=5):
    if not is_valid_post_url(post_url):
        print(f"유효하지 않은 URL 건너뜀: {post_url}")
        return {"text": "유효하지 않은 URL", "images": []}

    try:
        headers = get_headers()
        response = requests.get(post_url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = Soup(response.text, "html.parser")
        print(f"크롤링 중: {post_url}")
    except Exception as e:
        print(f"게시글 페이지 로드 오류: {post_url} - {str(e)}")
        return {"text": f"로드 오류: {str(e)}", "images": []}

    content_div = soup.find("div", class_="write_div") or soup.find("div", class_="writing_view_box")
    if not content_div:
        return {"text": "내용을 찾을 수 없습니다.", "images": []}

    text_content = content_div.get_text(separator="\n", strip=True)
    image_urls = [img.get("src") for img in content_div.find_all("img") if img.get("src")]
    image_urls = [urljoin("https://gall.dcinside.com", url) for url in image_urls]
    time.sleep(delay)
    return {"text": text_content, "images": image_urls}

# 게시판 목록 크롤링
# 게시판 목록 크롤링
def dcinside_peoplepower_crawl(url: str = 'https://gall.dcinside.com/mgallery/board/lists/?id=alliescon',
                         delay: int = 5,
                         min_views: int = 30000):  # 최소 조회수 기본값 1000으로 설정
    try:
        headers = get_headers()
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = Soup(response.text, "html.parser")
        print(f"목록 페이지 로드 완료: {url}")
        # print(f"응답 내용 (처음 500자): {response.text[:500]}")
    except Exception as e:
        print(f"목록 페이지 로드 오류: {str(e)}")
        print(f"페이지 소스 (처음 500자): {response.text[:500]}")
        return None

    board = soup.find("tbody", class_="listwrap2")
    if not board:
        print("게시판 데이터를 찾을 수 없습니다.")
        print(f"페이지 소스 (처음 500자): {response.text[:500]}")
        return None

    data = []
    for post in board.find_all("tr", class_="ub-content"):
        # 공지사항인지 확인 (고정 공지)
        post_num = post.find("td", class_="gall_num").text.strip()
        if post_num == "공지" or post_num == "설문" or post_num == "이벤트":
            continue
            
        # 제목에 광고 표시가 있는지 확인
        title_elem = post.find("td", class_="gall_tit ub-word")
        if not title_elem or not title_elem.find("a"):
            continue

        # 카테고리 체크 (공지 카테고리 제외)
        category_elem = title_elem.find("em", class_="icon_txt")
        category = category_elem.text.strip() if category_elem else "N/A"
        if category == "공지" or category == "AD" or category == "광고":
            continue

        # 제목 추출 부분 추가
        a_tag = title_elem.find("a")
        title = a_tag.get_text(strip=True)
        # 카테고리가 제목에 포함된 경우 제거
        if category_elem and category != "N/A":
            title = title.replace(f"[{category}]", "").strip()

        href = a_tag["href"]
        link = urljoin("https://gall.dcinside.com", href)
        writer = post.find("td", class_="gall_writer").text.strip()
        date_elem = post.find("td", class_="gall_date")
        date_str = date_elem.get("title", date_elem.text.strip())
        views_str = post.find("td", class_="gall_count").text.strip()
        recommend_str = post.find("td", class_="gall_recommend").text.strip()

        # 카테고리 추가 처리
        category_elem = title_elem.find("em", class_="icon_txt")
        category = category_elem.text.strip() if category_elem else "N/A"

        try:
            if "/" in date_str:
                if len(date_str) == 8:
                    post_date = datetime.strptime(date_str, "%y/%m/%d")
                else:
                    post_date = datetime.strptime(date_str, "%y/%m/%d %H:%M")
            elif len(date_str) == 8 and date_str.count(".") == 2:
                date_str = f"20{date_str}"
                post_date = datetime.strptime(date_str, "%Y.%m.%d")
            elif len(date_str) > 10:
                post_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            else:
                post_date = datetime.strptime(f"{datetime.now().year}-{datetime.now().month}-{datetime.now().day} {date_str}", "%Y-%m-%d %H:%M")
        except ValueError:
            print(f"날짜 파싱 오류: {date_str}")
            continue

        views_num = int(views_str) if views_str.isdigit() else 0
        recommend_num = int(recommend_str) if recommend_str.isdigit() else 0

        # 최소 조회수 이상인 경우에만 추가
        if views_num >= min_views:
            content_data = get_post_content(link, delay=delay)
            time.sleep(random.uniform(3, 7))  # 차단 방지

            data.append({
                "Post ID": post_num,
                "Community": "1p",
                "Category": category,
                "Title": title,
                "Link": link,
                "Writer": writer,
                "Date": post_date,
                "Views": views_num,
                "Recommend": recommend_num,
                "Content": content_data["text"],
                "Images": content_data["images"]
            })

    if data:
        df = pd.DataFrame(data)
        df = df.sort_values(by="Date", ascending=False)
        return df
    return None

if __name__ == "__main__":
    # 오늘 날짜 폴더 경로 설정
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
    
    df = dcinside_peoplepower_crawl(delay=5, min_views=150)  # 최소 조회수 1000으로 설정
    if df is not None:
        print(df[["Post ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Content", "Images"]])
        
        # 오늘 날짜 폴더에 CSV 파일 저장
        file_name = f"dcinside_peoplepower_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 '{file_path}' 파일로 저장되었습니다.")

