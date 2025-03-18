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
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Referer": "https://www.fmkorea.com/",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0"
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

# 페이지 존재 여부 확인 - proxy 제거
def check_page_exists(url, headers):
    try:
        response = requests.get(url, headers=headers, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"페이지 접근 오류: {e}")
        return False

# 개별 게시글 크롤링
def get_post_content(post_url, max_retries=2, timeout=10):
    if not is_valid_post_url(post_url):
        return {"text": "유효하지 않은 URL", "images": []}

    attempt = 0
    time.sleep(random.uniform(3, 7))
    
    while attempt < max_retries:
        try:
            headers = get_headers()
            response = requests.get(post_url, headers=headers, timeout=timeout)
            response.raise_for_status()
            response.encoding = 'utf-8'  # 인코딩 명시
            
            soup = Soup(response.text, "html.parser")
            content_div = soup.find("div", class_="xe_content")
            
            if not content_div:
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
                    
            return {"text": filtered_text, "images": image_urls}
            
        except Exception as e:
            attempt += 1
            if attempt == max_retries:
                return {"text": f"로드 실패: {str(e)}", "images": []}
            time.sleep(random.uniform(1, 2))

# 게시판 크롤링 함수 (전체 게시글 수집)
def fmkorea_funnyboard_crawl(min_views=10000, max_pages=10):
    # 정확한 URL 형식 사용
    base_url = 'https://www.fmkorea.com/humor'
    data = []
    page = 1
    
    # 오늘 날짜 게시물을 찾지 못한 연속 페이지 수
    consecutive_empty_pages = 0
    max_consecutive_empty = 3  # 연속 3페이지 동안 오늘 날짜 게시글을 찾을 수 없으면 종료
    
    # 중복 체크를 위한 집합
    processed_links = set()

    print(f"크롤링 시작: 최소 조회수 {min_views}, 최대 {max_pages}페이지")
    print(f"오늘 날짜: {datetime.now().date()}")

    while page <= max_pages:
        # 페이지 URL 설정 - 올바른 URL 형식 사용
        if page == 1:
            page_url = base_url
        else:
            page_url = f"https://www.fmkorea.com/index.php?mid=humor&page={page}"
            
        print(f"\n페이지 {page} 크롤링 중: {page_url}")

        try:
            headers = get_headers()
            response = requests.get(page_url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = 'utf-8'
            soup = Soup(response.text, "html.parser")
            
            print(f"HTML 구조 확인: tbody 태그 존재 여부: {bool(soup.find('tbody'))}")
            
            tbody = soup.find("tbody")
            if not tbody:
                print(f"페이지 {page}에서 게시글 목록을 찾을 수 없습니다.")
                page += 1
                time.sleep(random.uniform(1, 2))
                continue

            # 게시글 목록 긁어오기
            posts = []
            today_posts_found = False
            skipped_posts = 0
            
            for post in tbody.find_all("tr"):
                if "notice" in post.get("class", []):
                    continue

                # 날짜 확인
                date_elem = post.find("td", class_="time")
                if not date_elem:
                    continue
                    
                date_str = date_elem.text.strip()
                try:
                    if ":" in date_str:  # 오늘 날짜
                        post_date = datetime.strptime(f"{datetime.now().date()} {date_str}", "%Y-%m-%d %H:%M")
                        today_posts_found = True
                    else:
                        skipped_posts += 1
                        continue  # 오늘 날짜가 아니면 제외
                except ValueError:
                    continue
                    
                # 조회수 확인
                views_elem = post.find("td", class_="m_no")
                if not views_elem or not views_elem.text.strip().isdigit():
                    continue
                    
                views = int(views_elem.text.strip())
                if views < min_views:
                    skipped_posts += 1
                    continue
                
                # 제목과 링크 추출
                title_elem = post.find("td", class_="title")
                if not title_elem or not title_elem.find("a"):
                    continue
                    
                link_elem = title_elem.find("a")
                link = link_elem.get("href", "")
                if not link:
                    continue
                    
                if not link.startswith("http"):
                    link = "https://www.fmkorea.com" + link
                
                if link in processed_links:
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
                
                print(f"조회수 {views}의 게시물 발견: {title}")
                
                posts.append({
                    "Post_ID": post_num,
                    "Community": "11",
                    "Category": category,
                    "Title": title,
                    "Link": link,
                    "Writer": writer,
                    "Date": post_date,
                    "Views": views,
                    "Recommend": recommend
                })
            
            print(f"페이지 {page}에서 발견된 총 게시글: {len(tbody.find_all('tr'))}개")
            print(f"오늘 날짜 아니거나 조회수 부족으로 건너뛴 게시글: {skipped_posts}개")
            print(f"조건에 맞는 게시글: {len(posts)}개")
            
            if not today_posts_found or len(posts) == 0:
                consecutive_empty_pages += 1
                print(f"페이지 {page}에서 오늘 날짜 게시글을 찾을 수 없습니다. ({consecutive_empty_pages}/{max_consecutive_empty})")
                if consecutive_empty_pages >= max_consecutive_empty:
                    print(f"연속 {max_consecutive_empty}페이지 동안 오늘 날짜 게시글을 찾을 수 없어 크롤링을 종료합니다.")
                    break
            else:
                consecutive_empty_pages = 0  # 오늘 날짜 게시글 찾으면 카운터 초기화
                print(f"페이지 {page}에서 오늘 날짜 게시글을 발견했습니다. 연속 카운터 초기화.")

            # 수집된 게시글 내용 가져오기
            for post in posts:
                content_data = get_post_content(post["Link"])
                post["Content"] = content_data["text"]
                post["Images"] = content_data["images"]
                data.append(post)
                processed_links.add(post["Link"])
                print(f"게시글 내용 크롤링 완료: {post['Title']}")
                time.sleep(random.uniform(1, 2))

            page += 1
            time.sleep(random.uniform(3, 7))

        except Exception as e:
            print(f"페이지 처리 중 오류: {e}")
            page += 1
            time.sleep(random.uniform(3, 7))

    print(f"\n크롤링 완료. 총 수집된 게시물 수: {len(data)}개")

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
    
    df = fmkorea_funnyboard_crawl(min_views=10000)
    if df is not None:
        available_cols = [col for col in ["Post_ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Content", "Images"] if col in df.columns]
        print(df[available_cols])
        
        file_name = f"fmkorea_funnyboard_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 '{file_path}' 파일로 저장되었습니다.")
    else:
        print("크롤링할 데이터가 없습니다.")
