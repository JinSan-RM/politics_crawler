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

# 개별 게시글 크롤링 (내용 및 이미지, 내부 날짜 확인)
def get_post_content(post_url, max_retries=2, timeout=10):
    if not is_valid_post_url(post_url):
        print(f"유효하지 않은 URL 건너뜀: {post_url}")
        return {"text": "유효하지 않은 URL", "images": []}

    attempt = 0
    time.sleep(random.uniform(3, 7))
    
    while attempt < max_retries:
        try:
            headers = get_headers()
            print(f"사용 중인 User-Agent: {headers['User-Agent'][:30]}...")
            response = requests.get(post_url, headers=headers, timeout=timeout)
            response.raise_for_status()
            response.encoding = 'utf-8'
            
            soup = Soup(response.text, "html.parser")
            # 게시글 상단 날짜 정보 확인 (예: DCInside 형식)
            head_div = soup.find("div", class_="gallview_head")
            if head_div:
                date_elem = head_div.find("span", class_="gall_date")
                if date_elem:
                    date_text = date_elem.get("title", date_elem.text.strip())
                    try:
                        post_date = datetime.strptime(date_text, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        print(f"날짜 포맷 파싱 오류: {date_text}")
                        post_date = None
                    # 만약 내부 게시글 날짜가 오늘이 아니라면
                    if post_date and post_date.date() != datetime.now().date():
                        print(f"게시글 내부 날짜({post_date.date()})가 오늘({datetime.now().date()})이 아님. 건너뜁니다.")
                        return {"text": "오늘 날짜 게시글이 아님", "images": []}
            else:
                print("게시글 상단 날짜 정보를 찾을 수 없습니다.")
                # 날짜 정보가 없으면 계속 진행 (원하는 경우 여기서 건너뛸 수 있음)
            
            # FM코리아 게시글 내용 영역 (예시로 xe_content 사용)
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

# 게시판 크롤링 함수 (FM코리아 재미게시판)
def fmkorea_funnyboard_crawl(min_views=10000, max_pages=10):
    base_url = 'https://www.fmkorea.com/humor'
    data = []
    page = 1
    
    consecutive_empty_pages = 0
    max_consecutive_empty = 5  # 연속 3페이지 동안 오늘 게시글 없으면 종료
    
    processed_links = set()

    print(f"크롤링 시작: 최소 조회수 {min_views}, 최대 {max_pages}페이지")
    print(f"오늘 날짜: {datetime.now().date()}")

    # 내부에서 post_content를 호출했을 때 오늘 날짜가 아닌 게시글의 연속 건수를 저장하는 변수
    inside_not_today_count = 0

    while page <= max_pages:
        if page == 1:
            page_url = base_url
        else:
            page_url = f"https://www.fmkorea.com/index.php?mid=humor&page={page}"
            
        print(f"\n페이지 {page} 크롤링 중: {page_url}", flush=True)

        try:
            headers = get_headers()
            print(f"사용 중인 User-Agent: {headers['User-Agent'][:30]}...")
            response = requests.get(page_url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = 'utf-8'
            soup = Soup(response.text, "html.parser")
            
            tbody = soup.find("tbody")
            if not tbody:
                print(f"페이지 {page}에서 게시글 목록을 찾을 수 없습니다.")
                page += 1
                time.sleep(random.uniform(1, 2))
                continue

            posts = []
            today_posts_found = False
            skipped_posts = 0
            
            for post in tbody.find_all("tr"):
                if "notice" in post.get("class", []):
                    continue

                date_elem = post.find("td", class_="time")
                if not date_elem:
                    continue
                date_str = date_elem.text.strip()
                try:
                    if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                        try:
                            post_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            post_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
                    else:
                        post_date = datetime.strptime(f"{datetime.now().date()} {date_str}", "%Y-%m-%d %H:%M")
                        today_posts_found = True
                except ValueError:
                    print(f"날짜 파싱 오류: {date_str}")
                    continue

                if post_date.date() != datetime.now().date():
                    skipped_posts += 1
                    continue
                else:
                    today_posts_found = True

                views_elem = post.find("td", class_="m_no")
                if not views_elem or not views_elem.text.strip().isdigit():
                    continue
                views = int(views_elem.text.strip())
                if views < min_views:
                    skipped_posts += 1
                    continue

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

                cate_elem = post.find("td", class_="cate")
                category = ""
                if cate_elem and cate_elem.find("a"):
                    category = filter_korean_english(cate_elem.find("a").text.strip())

                writer_elem = post.find("td", class_="author")
                writer = filter_korean_english(writer_elem.text.strip()) if writer_elem else ""

                recommend_elem = post.find_all("td", class_="m_no")
                recommend = 0
                if len(recommend_elem) > 1:
                    recommend_text = recommend_elem[-1].text.strip()
                    if recommend_text.isdigit():
                        recommend = int(recommend_text)

                print(f"조회수 {views}의 게시물 발견: {title}")
                
                posts.append({
                    "Post ID": post_num,
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
                print(f"페이지 {page}에서 오늘 날짜 게시글을 찾을 수 없습니다. ({consecutive_empty_pages}/{max_consecutive_empty})", flush=True)
                if consecutive_empty_pages >= max_consecutive_empty:
                    print(f"연속 {max_consecutive_empty}페이지 동안 오늘 날짜 게시글을 찾을 수 없어 크롤링을 종료합니다.", flush=True)
                    break
            else:
                consecutive_empty_pages = 0
                print(f"페이지 {page}에서 오늘 날짜 게시글을 발견했습니다. 연속 카운터 초기화.", flush=True)

            # 필터링된 게시글 내용 가져오기
            for post in posts:
                content_data = get_post_content(post["Link"])
                # 내부 크롤링 결과가 '오늘 날짜 게시글이 아님'이면 내부 카운터 증가
                if content_data["text"] == "오늘 날짜 게시글이 아님":
                    inside_not_today_count += 1
                    print(f"내부 날짜 검사: {post['Title']} - 오늘 날짜 아님 (연속 {inside_not_today_count}회)")
                    if inside_not_today_count >= 3:
                        print("내부에서 오늘 날짜 게시글이 아닌 게시물이 연속 3개 발견되어 크롤링 종료합니다.", flush=True)
                        break
                    continue  # 이 게시글은 데이터에 추가하지 않음
                else:
                    # 유효한 게시글이면 내부 카운터 초기화
                    inside_not_today_count = 0

                post["Content"] = content_data["text"]
                post["Images"] = content_data["images"]
                data.append(post)
                processed_links.add(post["Link"])
                print(f"게시글 내용 크롤링 완료: {post['Title']}")
                time.sleep(random.uniform(1, 2))

            if inside_not_today_count >= 5:
                break

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
    
    df = fmkorea_funnyboard_crawl(min_views=200, max_pages=30)
    if df is not None:
        available_cols = [col for col in ["Post_ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Content", "Images"] if col in df.columns]
        print(df[available_cols])
        
        file_name = f"fmkorea_funnyboard_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 '{file_path}' 파일로 저장되었습니다.")
    else:
        print("크롤링할 데이터가 없습니다.")
