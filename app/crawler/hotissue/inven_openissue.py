import pandas as pd
from datetime import datetime
import time
import requests
from bs4 import BeautifulSoup as Soup
import random
import os

# 헤더 설정 함수
def get_headers():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0",
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://www.inven.co.kr/",
    }

# 유효한 URL 확인 함수
def is_valid_post_url(url):
    if not url or "javascript:" in url:
        return False
    return url.startswith("http")

# 게시글 내용 크롤링 함수 (BeautifulSoup만 사용)
def get_post_content(post_url, delay=2):
    print(f"\n[게시글 접근] URL: {post_url}")
    
    if not is_valid_post_url(post_url):
        print(f"[유효하지 않은 URL] 건너뜀: {post_url}")
        return {"text": "유효하지 않은 URL", "images": [], "actual_date": None}

    try:
        headers = get_headers()
        print(f"[요청 전송] User-Agent: {headers['User-Agent'][:30]}...")
        response = requests.get(post_url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = 'utf-8'  # 인코딩 명시
        print(f"[응답 성공] 상태 코드 {response.status_code}, 응답 크기 {len(response.text)} 바이트")
        
        soup = Soup(response.text, "html.parser")
    except requests.exceptions.Timeout:
        print(f"[요청 타임아웃] URL: {post_url}")
        return {"text": "요청 타임아웃", "images": [], "actual_date": None}
    except Exception as e:
        print(f"[페이지 로드 오류] URL: {post_url} - {str(e)}")
        return {"text": f"로드 오류: {str(e)}", "images": [], "actual_date": None}

    # 게시글 실제 날짜 확인
    actual_date = None
    date_elem = soup.find("div", class_="articleDate")
    if date_elem:
        date_str = date_elem.text.strip()
        try:
            actual_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            print(f"[날짜 확인] 게시글 실제 날짜: {actual_date}")
        except ValueError:
            print(f"[날짜 파싱 오류] 날짜 문자열: {date_str}")
    
    # 게시글 내용 영역 찾기
    content_div = soup.find("div", id="powerbbsContent") or soup.find("div", class_="contentBody")
    if not content_div:
        print(f"[내용 영역 없음] URL: {post_url}")
        return {"text": "내용을 찾을 수 없습니다.", "images": [], "actual_date": actual_date}

    text_content = content_div.get_text(separator="\n", strip=True)
    print(f"[텍스트 추출 성공] 글자 수: {len(text_content)}")
    
    # 이미지 URL 추출
    image_urls = []
    for img in content_div.find_all("img"):
        if img.get("src"):
            img_url = img.get("src")
            if img_url.startswith("//"):
                img_url = f"https:{img_url}"
            image_urls.append(img_url)
    
    # 비디오 URL 추출
    video_urls = []
    for video in content_div.find_all("video"):
        if video.get("src"):
            video_urls.append(video.get("src"))
    
    # 모든 미디어 URL 합치기
    all_media_urls = image_urls + video_urls
    print(f"[미디어 추출 성공] 이미지 수: {len(image_urls)}, 비디오 수: {len(video_urls)}")
    
    time.sleep(delay)
    return {
        "text": text_content,
        "images": all_media_urls,
        "actual_date": actual_date,
    }

# 인벤 게시판 크롤링 함수 (오늘 날짜만)
def inven_board_crawl(url='https://www.inven.co.kr/board/webzine/2097', delay=2, min_views=2000, max_pages=5, max_consecutive_not_today=3):
    today = datetime.now().date()
    data = []
    
    page = 1
    consecutive_empty_pages = 0  # 연속 빈 페이지 카운터
    max_consecutive_empty = 3  # 연속 빈 페이지 제한
    consecutive_not_today_posts = 0  # 오늘 날짜가 아닌 게시글 연속 카운터

    print(f"\n[크롤링 시작] 오늘 날짜: {today}, 최소 조회수: {min_views}")
    print(f"[설정] 연속 오늘 날짜 아닌 게시글 제한: {max_consecutive_not_today}개")

    while page <= max_pages:
        page_url = f"{url}?p={page}"
        print(f"\n[페이지 접근] 페이지 {page}: {page_url}")

        try:
            headers = get_headers()
            response = requests.get(page_url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = 'utf-8'
            soup = Soup(response.text, 'html.parser')
            print(f"[페이지 로드 성공] 상태 코드: {response.status_code}")
            
            # 게시판 테이블 찾기
            board_table = soup.find('table', class_='board_list')
            if not board_table:
                print("[게시판 테이블 없음] 다른 선택자로 시도합니다.")
                board_table = soup.find('table')
                
            tbody = board_table.find('tbody') if board_table else None
            
            if not tbody:
                print("[HTML 구조 확인] 테이블 구조 출력:")
                if board_table:
                    print(board_table.prettify()[:500])
                else:
                    print(soup.prettify()[:500])
                    
                consecutive_empty_pages += 1
                print(f"[빈 페이지 발견] 연속 빈 페이지 수: {consecutive_empty_pages}/{max_consecutive_empty}")
                if consecutive_empty_pages >= max_consecutive_empty:
                    print("[크롤링 종료] 빈 페이지가 연속으로 발생했습니다.")
                    break
                page += 1
                continue
            
            consecutive_empty_pages = 0  # 빈 페이지 카운터 초기화
            
            all_posts = tbody.find_all('tr')
            print(f"[게시글 목록] 발견된 총 게시글 수: {len(all_posts)}개")
            
            for post in all_posts:
                # 공지사항 제외
                if post.get('class') and ('notice' in post.get('class') or 'notice_pop' in post.get('class')):
                    print("[공지사항 제외]")
                    continue
                
                # 조회수 확인
                views_elem = post.find('td', class_='hit') or post.find('td', class_='view')
                if not views_elem:
                    print("[조회수 요소 없음]")
                    continue
                    
                views_text = views_elem.text.strip().replace(',', '')
                if not views_text.isdigit():
                    print(f"[조회수 형식 오류] {views_text}")
                    continue
                
                views = int(views_text)
                if views < min_views:
                    print(f"[조회수 미달] {views} < {min_views}")
                    continue
                
                # 제목 및 링크
                title_elem = post.find('a', class_='subject-link') or post.find('td', class_='tit').find('a')
                if not title_elem or not title_elem.get('href'):
                    print("[제목 요소 없음]")
                    continue
                
                title = title_elem.text.strip()
                link = title_elem['href']
                if not link.startswith('http'):
                    link = f"https://www.inven.co.kr{link}"
                
                print(f"[게시글 발견] 제목: {title}, 조회수: {views}")
                
                # 게시글 내용 및 실제 날짜 확인
                content_data = get_post_content(link, delay=delay)
                
                # 실제 날짜가 오늘인지 확인
                actual_date = content_data['actual_date']
                if not actual_date:
                    print(f"[날짜 확인 불가] 게시글: {title}")
                    consecutive_not_today_posts += 1
                    print(f"[연속 오늘 아닌 게시글] {consecutive_not_today_posts}/{max_consecutive_not_today}")
                    
                    if consecutive_not_today_posts >= max_consecutive_not_today:
                        print(f"[크롤링 종료] 연속 {max_consecutive_not_today}개의 오늘 날짜 아닌 게시글 발견")
                        break
                    continue
                
                if actual_date.date() != today:
                    print(f"[오늘 날짜 아님] 게시글 날짜: {actual_date.date()}, 오늘 날짜: {today}")
                    consecutive_not_today_posts += 1
                    print(f"[연속 오늘 아닌 게시글] {consecutive_not_today_posts}/{max_consecutive_not_today}")
                    
                    if consecutive_not_today_posts >= max_consecutive_not_today:
                        print(f"[크롤링 종료] 연속 {max_consecutive_not_today}개의 오늘 날짜 아닌 게시글 발견")
                        break
                    continue
                
                # 오늘 날짜 게시글 처리
                consecutive_not_today_posts = 0  # 오늘 날짜 게시글 발견시 카운터 초기화
                
                # 작성자 정보 추출
                writer_elem = post.find('td', class_='user') or post.find('td', class_='name')
                writer = "N/A"
                if writer_elem:
                    writer = writer_elem.text.strip()
                
                # 추천수 추출
                recommend_elem = post.find('td', class_='reco') or post.find('td', class_='recom')
                recommend = 0
                if recommend_elem and recommend_elem.text.strip():
                    recommend_text = recommend_elem.text.strip().replace(',', '')
                    if recommend_text.isdigit():
                        recommend = int(recommend_text)
                
                # 카테고리 추출
                category_elem = post.find('span', class_='category') or post.find('td', class_='category')
                category = category_elem.text.strip() if category_elem else "N/A"
                
                post_id = link.split('/')[-1].split('?')[0] if '/' in link else "N/A"
                
                data.append({
                    'Post ID': post_id,
                    "Community": "10",
                    'Category': category,
                    'Title': title,
                    'Link': link,
                    'Writer': writer,
                    'Date': actual_date,
                    'Recommend': recommend,
                    'Views': views,
                    'Content': content_data['text'],
                    'Images': content_data['images'],
                })
                print(f"[게시글 추가됨] 제목: {title}, 조회수: {views}, 날짜: {actual_date}")
            
            # 연속 오늘 날짜 아닌 게시글 제한 초과 확인
            if consecutive_not_today_posts >= max_consecutive_not_today:
                break
            
            page += 1
            time.sleep(random.uniform(3, 7))
        
        except Exception as e:
            consecutive_empty_pages += 1
            print(f"[페이지 로드 오류] 페이지 {page}: {str(e)}")
            if consecutive_empty_pages >= max_consecutive_empty:
                print("[크롤링 종료] 빈 페이지가 연속으로 발생했습니다.")
                break
            time.sleep(random.uniform(3, 7))
    
    print(f"\n[크롤링 완료] 총 수집된 게시글: {len(data)}개")
    
    df = pd.DataFrame(data)
    return df

if __name__ == '__main__':
    inven_url = 'https://www.inven.co.kr/board/webzine/2097'
    
    df = inven_board_crawl(
        url=inven_url, 
        delay=2, 
        min_views=2000, 
        max_pages=5,
        max_consecutive_not_today=3  # 오늘 날짜가 아닌 게시글이 연속 3개 이상이면 종료
    )
    
    if df is not None and not df.empty:
        available_cols = [col for col in ["Post_ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Content", "Images"] if col in df.columns]
        print(df[available_cols])
        today_str = datetime.now().strftime('%Y%m%d')
        
        save_path = f'/code/data/{today_str}/inven_openissue_{today_str}.csv'
        
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        df.to_csv(save_path, index=False, encoding='utf-8-sig')
        
        print(f"[크롤링 완료] 데이터 저장 경로: {save_path}")
