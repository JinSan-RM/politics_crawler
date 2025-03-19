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
        "Upgrade-Insecure-Requests": "1"
    }

# 유효한 URL 확인
def is_valid_post_url(url):
    if not url or "javascript:" in url:
        return False
    return url.startswith("http")

# 게시글 내용 크롤링 (Selenium 사용)
# 게시글 내용 크롤링 (BeautifulSoup만 사용)
def get_post_content(post_url, delay=5):
    if not is_valid_post_url(post_url):
        print(f"유효하지 않은 URL 건너뜀: {post_url}")
        return {"text": "유효하지 않은 URL", "images": [], "recommend": "0"}

    try:
        headers = get_headers()
        response = requests.get(post_url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        soup = Soup(response.text, "html.parser")
        print(f"크롤링 중: {post_url}")
    except Exception as e:
        print(f"게시글 페이지 로드 오류: {post_url} - {str(e)}")
        return {"text": f"로드 오류: {str(e)}", "images": [], "recommend": "0"}

    # 추천수 추출
    recommend_elem = soup.find("span", {"id": "likeCnt"})
    recommend = recommend_elem.text if recommend_elem else "0"

    # 게시글 내용 추출
    content_div = soup.find("div", class_="view_context")
    if not content_div:
        print(f"내용 영역을 찾을 수 없습니다: {post_url}")
        return {"text": "내용을 찾을 수 없습니다.", "images": [], "recommend": recommend}

    text_content = content_div.find("div", class_="ar_txt").get_text(separator="\n", strip=True)
    print(f"추출된 텍스트 (처음 100자): {text_content[:100]}")

    # 이미지 URL 추출 (광고 및 yellow.contentsfeed.com 제외)
    image_urls = []
    for img in content_div.find_all("img"):
        src = img.get("src")
        if src and "yellow.contentsfeed.com" not in src:
            # 광고 div 내부의 이미지 제외
            if not img.find_parent("div", style="background:#f8f7f7;"):
                image_urls.append(src)
    
    image_urls = ["https:" + url if url.startswith("//") else url for url in image_urls]
    print(f"추출된 이미지 URL: {image_urls}")

    time.sleep(delay)
    return {"text": text_content, "images": image_urls, "recommend": recommend}

def clean_text(text):
    """텍스트 내의 불필요한 공백과 특수 문자를 제거합니다."""
    text = text.strip()
    text = html.unescape(text)  # HTML 엔티티 디코딩
    return text

def get_next_page_url(page_num):
    # 기본 URL 구조
    base_url = "https://mlbpark.donga.com/mp/b.php?m=list&b=bullpen&query=&select=&subquery=&subselect=&user="
    # 페이지 번호 계산 (1페이지: p=1, 2페이지: p=31, 3페이지: p=61...)
    p_value = (page_num - 1) * 30 + 1
    next_url = f"{base_url}&p={p_value}"
    return next_url

def mlbpark_board_crawl(url: str = 'https://mlbpark.donga.com/mp/b.php?p=1&m=list&b=bullpen&query=&select=&subquery=&subselect=&user=',
                        delay: int = 5,
                        min_views: int = 500):  # 조회수 300 이상으로 설정
    today = datetime.now().date()
    data = []
    
    # 중복 체크를 위한 집합
    post_ids_set = set()
    post_links_set = set()
    
    # 오늘 날짜 게시물을 찾지 못한 연속 페이지 수
    consecutive_empty_pages = 0
    max_consecutive_empty = 3  # 연속 3페이지 동안 오늘 게시글 없으면 종료
    
    # 페이지 번호 (1부터 시작)
    page_num = 1
    
    while True:
        page_url = get_next_page_url(page_num)
        print(f"페이지 {page_num} 크롤링 중 (p={(page_num-1)*30+1}): {page_url}")
        
        try:
            headers = get_headers()
            response = requests.get(page_url, headers=headers, timeout=10)
            response.encoding = 'utf-8'
            soup = Soup(response.text, "html.parser")
            print(f"목록 페이지 로드 완료: {page_url}")
        except Exception as e:
            print(f"페이지 로드 오류: {str(e)}")
            break

        board = soup.find("table", class_="tbl_type01")
        if not board:
            print("게시판 데이터를 찾을 수 없습니다.")
            break

        today_posts_found = False
        
        for post in board.find_all("tr"):
            if "notice" in post.get("class", []):
                continue
            
            try:
                # 날짜 확인
                date_elem = post.find("span", class_="date")
                if not date_elem:
                    continue
                
                date_str = date_elem.text.strip()
                
                # 오늘 날짜 게시물만 처리 (HH:MM:SS 형식)
                if ":" in date_str:
                    today_posts_found = True
                    
                    # 조회수 확인
                    views_elem = post.find("span", class_="viewV")
                    if not views_elem or not views_elem.text.strip().isdigit():
                        continue
                    
                    views = int(views_elem.text.strip())
                    if views < min_views:
                        continue  # 최소 조회수보다 작으면 제외
                    
                    # 게시물 정보 추출
                    title_elem = post.find("div", class_="tit")
                    if not title_elem or not title_elem.find("a"):
                        continue
                    
                    title_a = title_elem.find("a")
                    title = clean_text(title_a.text)
                    link = title_a["href"]
                    
                    # 중복 체크
                    if link in post_links_set:
                        print(f"중복된 링크 건너뜀: {link}")
                        continue
                    
                    post_id_elem = post.find("td", class_="t_left")
                    if not post_id_elem:
                        continue
                    
                    post_id = post_id_elem['id'] if post_id_elem.has_attr('id') else "N/A"
                    
                    if post_id in post_ids_set and post_id != "N/A":
                        print(f"중복된 Post ID 건너뜀: {post_id}")
                        continue
                    
                    writer_elem = post.find("span", class_="nick")
                    writer = clean_text(writer_elem.text) if writer_elem else "N/A"
                    
                    post_date = datetime.strptime(f"{today.strftime('%Y-%m-%d')} {date_str}", "%Y-%m-%d %H:%M:%S")
                    
                    print(f"조회수 {views}의 게시물 발견: {title}")
                    
                    # 카테고리 추가 (없으면 "N/A")
                    category_elem = post.find("span", class_="category")
                    category = category_elem.text.strip() if category_elem else "N/A"
                    
                    # 게시글 내용 크롤링
                    content_data = get_post_content(link, delay=delay)
                    if content_data["text"] not in ["내용을 찾을 수 없습니다.", "유효하지 않은 URL"] and not content_data["text"].startswith("로드 오류"):
                        post_ids_set.add(post_id)
                        post_links_set.add(link)
                        
                        data.append({
                            "Post ID": post_id,
                            "Community": "9",
                            "Category": category,
                            "Title": title,
                            "Link": link,
                            "Writer": writer,
                            "Date": post_date,
                            "Recommend": content_data["recommend"],
                            "Views": views,
                            "Content": content_data["text"],
                            "Images": content_data["images"]
                        })
                    else:
                        print(f"게시글 내용 추출 실패, 제외됨: {link}")
                    
                    time.sleep(random.uniform(1, 3))
                    
            except Exception as e:
                print(f"데이터 추출 중 오류 발생: {e}")
                continue
        
        print(f"페이지 {page_num} 처리 완료")
        
        # 오늘 날짜 게시물이 없으면 카운터 증가
        if not today_posts_found:
            consecutive_empty_pages += 1
            print(f"페이지 {page_num}에서 오늘 날짜 게시글을 찾을 수 없습니다. ({consecutive_empty_pages}/{max_consecutive_empty})")
            if consecutive_empty_pages >= max_consecutive_empty:
                print(f"연속 {max_consecutive_empty}페이지 동안 오늘 날짜 게시글을 찾을 수 없어 크롤링을 종료합니다.")
                break
        else:
            consecutive_empty_pages = 0  # 오늘 날짜 게시물 찾으면 카운터 초기화
        
        page_num += 1
        time.sleep(random.uniform(2, 5))

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
    
    df = mlbpark_board_crawl(delay=5, min_views=500)  # 조회수 300 이상으로 설정
    if df is not None:
        available_cols = [col for col in ["Post_ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Content", "Images"] if col in df.columns]
        print(df[available_cols])
        # 오늘 날짜 폴더에 CSV 파일 저장
        file_name = f"mlbpark_bullpen_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 '{file_path}' 파일로 저장되었습니다.")
