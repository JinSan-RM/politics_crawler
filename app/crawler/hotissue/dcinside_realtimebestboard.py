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

    print(f"크롤링 시작: {post_url}")
    try:
        headers = get_headers()
        print(f"사용 중인 User-Agent: {headers['User-Agent'][:30]}...")
        response = requests.get(post_url, headers=headers, timeout=10)
        response.raise_for_status()
        print(f"응답 상태 코드: {response.status_code}")
        soup = Soup(response.text, "html.parser")
        print(f"페이지 로딩 완료: {post_url}")
    except Exception as e:
        print(f"게시글 페이지 로드 오류: {post_url} - {str(e)}")
        return {"text": f"로드 오류: {str(e)}", "images": []}

    content_div = soup.find("div", class_="write_div") or soup.find("div", class_="writing_view_box")
    if not content_div:
        print(f"내용 영역을 찾을 수 없습니다: {post_url}")
        return {"text": "내용을 찾을 수 없습니다.", "images": []}

    text_content = content_div.get_text(separator="\n", strip=True)
    print(f"추출된 텍스트 길이: {len(text_content)} 글자")
    print(f"추출된 텍스트 (처음 100자): {text_content[:100]}")
    
    image_urls = [img.get("src") for img in content_div.find_all("img") if img.get("src")]
    image_urls = [urljoin("https://gall.dcinside.com", url) for url in image_urls]
    print(f"추출된 이미지 수: {len(image_urls)}")
    print(f"추출된 이미지 URL: {image_urls}")
    
    time.sleep(delay)
    print(f"크롤링 완료: {post_url}")
    return {"text": text_content, "images": image_urls}

# 게시판 목록 크롤링
def dcinside_realtimebest_crawl(url: str = 'https://gall.dcinside.com/board/lists/?id=dcbest',
                         min_views: int = 30000,  # 최소 조회수 30000으로 설정
                         delay: int = 5):
    today = datetime.now().date()
    print(f"오늘 날짜: {today}")
    print(f"최소 조회수 기준: {min_views}")
    data = []
    page = 1
    
    # 중복 체크를 위한 집합
    processed_links = set()
    
    # 오늘 날짜 게시물을 찾지 못한 연속 페이지 수
    consecutive_empty_pages = 0
    max_consecutive_empty = 3  # 연속 3페이지 동안 오늘 게시글 없으면 종료
    
    while True:
        page_url = f"{url}&page={page}"
        print(f"\n{'='*50}")
        print(f"페이지 {page} 크롤링 중: {page_url}")
        
        try:
            headers = get_headers()
            print(f"사용 중인 User-Agent: {headers['User-Agent'][:30]}...")
            response = requests.get(page_url, headers=headers, timeout=10)
            response.raise_for_status()
            print(f"응답 상태 코드: {response.status_code}")
            soup = Soup(response.text, "html.parser")
            print(f"목록 페이지 로드 완료: {page_url}")
        except Exception as e:
            print(f"목록 페이지 로드 오류: {str(e)}")
            break

        board = soup.find("tbody", class_="listwrap2")
        if not board:
            print("게시판 데이터를 찾을 수 없습니다.")
            print(f"HTML 구조 확인 (처음 500자): {soup.prettify()[:500]}...")
            break

        today_posts_found = False
        filtered_posts = []
        skipped_date_posts = 0
        skipped_views_posts = 0
        
        posts = board.find_all("tr", class_="ub-content")
        print(f"페이지에서 발견된 게시물 수: {len(posts)}")
        
        for post in posts:
            # 날짜 확인 - 오늘 날짜인지 체크
            date_elem = post.find("td", class_="gall_date")
            date_str = date_elem.get("title", date_elem.text.strip()) if date_elem else ""
            
            if not date_str:
                print("날짜 정보를 찾을 수 없습니다.")
                continue
                
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
                    # HH:MM 형식은 오늘 날짜로 간주
                    post_date = datetime.strptime(f"{today.strftime('%Y-%m-%d')} {date_str}", "%Y-%m-%d %H:%M")
                    today_posts_found = True
                
                # 오늘 날짜가 아니면 제외
                if post_date.date() != today:
                    skipped_date_posts += 1
                    continue
                
                today_posts_found = True
            except ValueError:
                print(f"날짜 파싱 오류: {date_str}")
                continue
                
            # 조회수 확인 - 최소 조회수 이상인지 체크
            views_elem = post.find("td", class_="gall_count")
            if not views_elem:
                print("조회수 정보를 찾을 수 없습니다.")
                continue
                
            views_str = views_elem.text.strip()
            views_num = int(views_str) if views_str.isdigit() else 0
            
            if views_num < min_views:
                skipped_views_posts += 1
                continue  # 최소 조회수보다 작으면 제외
            
            # 여기까지 왔다면 오늘 날짜이고 조회수가 충분한 게시물
            post_num_elem = post.find("td", class_="gall_num")
            post_num = post_num_elem.text.strip() if post_num_elem else "N/A"
            
            title_elem = post.find("td", class_="gall_tit ub-word")
            if not title_elem or not title_elem.find("a"):
                print("제목 정보를 찾을 수 없습니다.")
                continue
                
            title = title_elem.find("a").text.strip()
            href = title_elem.find("a")["href"]
            link = urljoin("https://gall.dcinside.com", href)
            
            # 이미 처리한 링크인지 확인
            if link in processed_links:
                print(f"중복된 링크 건너뜀: {link}")
                continue
            
            writer_elem = post.find("td", class_="gall_writer")
            writer = writer_elem.text.strip() if writer_elem else "N/A"
            
            recommend_elem = post.find("td", class_="gall_recommend")
            recommend_str = recommend_elem.text.strip() if recommend_elem else "0"
            recommend_num = int(recommend_str) if recommend_str.isdigit() else 0
            
            # print(f"조회수 {views_num}의 게시물 발견: {title} (작성자: {writer})")
            
            filtered_posts.append({
                "Post ID": post_num,
                "Community": "1",
                "category": "N/A",
                "Title": title,
                "Link": link,
                "Writer": writer,
                "Date": post_date,
                "Views": views_num,
                "Recommend": recommend_num
            })
        
        print(f"날짜 필터링으로 건너뛴 게시물: {skipped_date_posts}")
        print(f"조회수 필터링으로 건너뛴 게시물: {skipped_views_posts}")
        
        # 오늘 날짜 게시물이 없으면 카운터 증가
        if not today_posts_found:
            consecutive_empty_pages += 1
            print(f"페이지 {page}에서 오늘 날짜 게시글을 찾을 수 없습니다. ({consecutive_empty_pages}/{max_consecutive_empty})")
            if consecutive_empty_pages >= max_consecutive_empty:
                print(f"연속 {max_consecutive_empty}페이지 동안 오늘 날짜 게시글을 찾을 수 없어 크롤링을 종료합니다.")
                break
        else:
            consecutive_empty_pages = 0  # 오늘 날짜 게시물 찾으면 카운터 초기화
        
        # 필터링된 게시물의 내용 가져오기
        for post in filtered_posts:
            content_data = get_post_content(post["Link"], delay=delay)
            post["Content"] = content_data["text"]
            post["Images"] = content_data["images"]
            data.append(post)
            processed_links.add(post["Link"])  # 처리 완료된 링크 추가
            time.sleep(random.uniform(1, 3))  # 요청 간격 조정

        print(f"페이지 {page} 처리 완료. 필터링된 게시물 수: {len(filtered_posts)}")
        print(f"현재까지 수집된 총 게시물 수: {len(data)}")
        print(f"처리 완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        page += 1
        time.sleep(random.uniform(2, 5))  # 페이지 간 간격 조정

    print(f"\n{'='*50}")
    print(f"크롤링 완료. 총 수집된 게시물 수: {len(data)}")
    print(f"완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
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
    
    df = dcinside_realtimebest_crawl(delay=5, min_views=30000)   # 조회수 10000 이상으로 설정
    print(f"디시 실시간베스트 갤러리 : \n{df}")
    if df is not None:
        available_cols = [col for col in ["Post ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Content", "Images"] if col in df.columns]
        print(df[available_cols])
        
        # 오늘 날짜 폴더에 CSV 파일 저장
        file_name = f"dcinside_bestboard_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 '{file_path}' 파일로 저장되었습니다.")

