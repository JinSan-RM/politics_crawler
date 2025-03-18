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
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://www.bobaedream.co.kr/"
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
def get_post_content(post_url, delay=5):
    if not is_valid_post_url(post_url):
        print(f"유효하지 않은 URL 건너뜀: {post_url}")
        return {"text": "유효하지 않은 URL", "images": []}

    try:
        headers = get_headers()
        response = requests.get(post_url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = 'utf-8'  # 인코딩 명시
        soup = Soup(response.text, "html.parser")
        print(f"크롤링 중: {post_url}")
    except Exception as e:
        print(f"게시글 페이지 로드 오류: {post_url} - {str(e)}")
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
    return {"text": text_content, "images": image_urls}

# 보배드림 베스트 게시판 크롤링 (오늘 날짜만)
def bobaedream_bestboard_crawl(url: str = 'https://www.bobaedream.co.kr/list?code=best', 
                              delay: int = 5, 
                              min_views: int = 1000):
    today = datetime.now().date()
    data = []
    page = 1

    while True:
        page_url = f"{url}&page={page}" if page > 1 else url
        try:
            headers = get_headers()
            response = requests.get(page_url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = 'utf-8'
            soup = Soup(response.text, "html.parser")
            print(f"목록 페이지 로드 완료: {page_url}")
        except Exception as e:
            print(f"목록 페이지 로드 오류: {str(e)}")
            break

        board = soup.find("table", id="boardlist")
        if not board:
            print("게시판 데이터를 찾을 수 없습니다.")
            break

        posts = []
        for post in board.find("tbody").find_all("tr", attrs={"itemtype": "http://schema.org/Article"}):
            # 공지사항 및 광고 제외
            if post.get("class") and "notice" in post.get("class"):
                continue

            post_id_elem = post.find("td", class_="num01")
            if not post_id_elem:
                post_id = extract_post_id(post.find("a", class_="bsubject")["href"])
            else:
                post_id = post_id_elem.text.strip()

            title_elem = post.find("a", class_="bsubject")
            if not title_elem:
                continue
            title = title_elem.text.strip()
            link = title_elem["href"]
            link = f"https://www.bobaedream.co.kr{link}" if not link.startswith('http') else link

            writer_elem = post.find("span", class_="author")
            writer = writer_elem.text.strip() if writer_elem else "N/A"

            date_elem = post.find("td", class_="date")
            date_str = date_elem.text.strip() if date_elem else ""
            
            # 날짜 형식이 HH:MM 인 경우에만 오늘 날짜로 처리
            if ":" in date_str:
                try:
                    post_date = datetime.strptime(f"{today.strftime('%Y-%m-%d')} {date_str}", "%Y-%m-%d %H:%M")
                except ValueError:
                    print(f"날짜 파싱 오류: {date_str}")
                    continue
            else:
                # HH:MM 형식이 아니면, 해당 게시글은 건너뜀
                continue

            category_elem = post.find("td", class_="category")
            category = category_elem.text.strip() if category_elem else "N/A"

            recommend_elem = post.find("td", class_="recomm")
            recommend = "0"
            if recommend_elem:
                font_elem = recommend_elem.find('font')
                if font_elem:
                    recommend = font_elem.text.strip()

            views_elem = post.find("td", class_="count")
            views = int(views_elem.text.strip().replace(',', '')) if views_elem and views_elem.text.strip().replace(',', '').isdigit() else 0

            # 최소 조회수 이상인 경우에만 추가
            if views >= min_views:
                posts.append({
                    "Post ID": post_id,
                    "Community": "7p",
                    "Category": category,
                    "Title": title,
                    "Link": link,
                    "Writer": writer,
                    "Date": post_date,
                    "Recommend": recommend,
                    "Views": views
                })

        if not posts:
            print(f"페이지 {page}에서 오늘 날짜 게시글을 찾을 수 없습니다. 크롤링을 종료합니다.")
            break

        for post in posts:
            content_data = get_post_content(post["Link"], delay=delay)
            if content_data["text"] not in ["내용을 찾을 수 없습니다.", "유효하지 않은 URL"] and not content_data["text"].startswith("로드 오류"):
                post["Content"] = content_data["text"]
                post["Images"] = content_data["images"]
                data.append(post)
            else:
                print(f"게시글 내용 추출 실패, 제외됨: {post['Link']}")
            time.sleep(random.uniform(3, 7))

        print(f"페이지 {page} 처리 완료. 다음 페이지로 이동합니다.")
        page += 1
        time.sleep(random.uniform(3, 7))

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
    
    df = bobaedream_bestboard_crawl(delay=5, min_views=1000)  # 최소 조회수 10000으로 설정
    if df is not None:
        print(df[["Post ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Content", "Images"]])
        
        # 오늘 날짜 폴더에 CSV 파일 저장
        file_name = f"bobaedream_bestboard_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 '{file_path}' 파일로 저장되었습니다.")
