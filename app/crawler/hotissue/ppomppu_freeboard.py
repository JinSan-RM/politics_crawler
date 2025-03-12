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
        "Upgrade-Insecure-Requests": "1"
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
        return {"text": "유효하지 않은 URL", "images": []}

    try:
        headers = get_headers()
        response = requests.get(post_url, headers=headers, timeout=10)
        response.encoding = 'euc-kr'
        html_content = response.text
        
        # 디버깅: HTML 일부 출력
        print(f"HTML 일부: {html_content[:500]}")
        
        soup = Soup(html_content, "html.parser")
        
        # 1. 원래 선택자 시도
        content_td = soup.select_one("td.board-contents")
        print(f"content_td find (방법1): {content_td}")
        
        # 2. 더 일반적인 선택자 시도
        if not content_td:
            content_td = soup.select_one("td[class*='board']")
            print(f"content_td find (방법2): {content_td}")
        
        # 3. 계층적 접근 시도
        if not content_td:
            tables = soup.select("table")
            print(f"찾은 테이블 수: {len(tables)}")
            for i, table in enumerate(tables):
                print(f"테이블 {i} 클래스: {table.get('class', '없음')}")
            
            # 특정 테이블 내부 탐색
            if len(tables) > 2:
                content_td = tables[2].select_one("td")
                print(f"content_td find (방법3): {content_td}")
        
        if not content_td:
            return {"text": "내용을 찾을 수 없습니다.", "images": []}
        
        # 텍스트 추출
        paragraphs = []
        for p in content_td.find_all('p'):
            p_text = p.get_text(strip=True)
            if p_text:
                paragraphs.append(p_text)
        
        text_content = "\n".join(paragraphs) if paragraphs else content_td.get_text(strip=True)
        
        # 이미지 추출
        image_urls = [img.get("src") for img in content_td.find_all("img") if img.get("src")]
        image_urls = ["https:" + url if url.startswith("//") else url for url in image_urls]
        
        return {"text": text_content, "images": image_urls}
        
    except Exception as e:
        print(f"오류 발생: {str(e)}")
        return {"text": f"오류 발생: {str(e)}", "images": []}




# 게시판 크롤링 (오늘 날짜만)
def ppomppu_politics_crawl(url: str = 'https://www.ppomppu.co.kr/zboard/zboard.php?id=issue',
                         delay: int = 5,
                         min_views: int = 1500):  # 최소 조회수 기본값 1500으로 설정
    today = datetime.now().date()
    data = []
    page = 1

    while True:
        page_url = f"{url}&page={page}" if page > 1 else url
        try:
            headers = get_headers()
            response = requests.get(page_url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = 'euc-kr'  # 인코딩 명시
            soup = Soup(response.text, "html.parser")
            print(f"목록 페이지 로드 완료: {page_url}")
        except Exception as e:
            print(f"목록 페이지 로드 오류: {str(e)}")
            break
                
        board = soup.find("table", id="revolution_main_table")
        if not board:
            print("게시판 데이터를 찾을 수 없습니다.")
            break

        posts = []
        all_today = True
        for post in board.find_all("tr", class_="baseList"):
            post_num_elem = post.find("td", class_="baseList-numb")
            if not post_num_elem or post_num_elem.text.strip() in ["공지", "알림"]:
                continue

            post_num = post_num_elem.text.strip()
            title_elem = post.find("a", class_="baseList-title")
            if not title_elem:
                continue
            title = title_elem.text.strip()
            href = title_elem["href"]
            link = "https://www.ppomppu.co.kr/zboard/" + href if href.startswith("view.php") else href

            writer_elem = post.find("a", class_="baseList-name")
            writer = writer_elem.text.strip() if writer_elem else "N/A"

            date_elem = post.find("time", class_="baseList-time")
            date_str = date_elem.text.strip() if date_elem else ""
            try:
                if ":" in date_str:
                    post_date = datetime.strptime(f"{today} {date_str}", "%Y-%m-%d %H:%M:%S")
                else:
                    post_date = datetime.strptime(date_str, "%y/%m/%d")
                if post_date.date() != today:
                    all_today = False
                    continue  # 오늘 날짜가 아니면 posts에 추가하지 않음
            except ValueError:
                print(f"날짜 파싱 오류: {date_str}")
                continue

            recommend_elem = post.find("td", class_="baseList-rec")
            recommend_str = recommend_elem.text.strip() if recommend_elem else "0 - 0"
            recommend = recommend_str if recommend_str else "0 - 0"

            views_elem = post.find("td", class_="baseList-views")
            views = int(views_elem.text.strip()) if views_elem and views_elem.text.strip().isdigit() else 0

            # 최소 조회수 이상인 경우에만 추가
            if views >= min_views:
                category_elem = post.find("span", class_="baseList-category")
                category = category_elem.text.strip() if category_elem else "N/A"

                posts.append({
                    "Post ID": post_num,
                    "Category": category,
                    "Title": title,
                    "Link": link,
                    "Writer": writer,
                    "Date": post_date,
                    "Recommend": recommend,
                    "Views": views
                })

        if not posts:
            print(f"페이지 {page}에서 오늘 날짜 게시글을 찾을 수 없습니다.")
            break

        for post in posts:
            content_data = get_post_content(post["Link"], delay=delay)
            # 내용이 제대로 추출된 경우에만 추가
            if content_data["text"] not in ["내용을 찾을 수 없습니다.", "유효하지 않은 URL"] and not content_data["text"].startswith("로드 오류"):
                post["Content"] = content_data["text"]
                post["Images"] = content_data["images"]
                data.append(post)
            else:
                print(f"게시글 내용 추출 실패, 제외됨: {post['Link']}")
            time.sleep(random.uniform(3, 7))

        if all_today:
            print(f"페이지 {page}의 모든 게시글이 오늘 날짜입니다. 다음 페이지로 이동합니다.")
            page += 1
            time.sleep(random.uniform(3, 7))
        else:
            print(f"페이지 {page}에서 오늘 날짜가 아닌 게시글을 발견했습니다. 크롤링을 종료합니다.")
            break

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
    
    df = ppomppu_politics_crawl(delay=5, min_views=1500)  # 최소 조회수 1500으로 설정
    if df is not None:
        available_cols = [col for col in ["Post_ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Content", "Images"] if col in df.columns]
        print(df[available_cols])
        # 오늘 날짜 폴더에 CSV 파일 저장
        file_name = f"ppomppu_freeboard_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 '{file_path}' 파일로 저장되었습니다.")
