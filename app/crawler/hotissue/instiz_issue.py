import requests
from bs4 import BeautifulSoup as Soup
import pandas as pd
from datetime import datetime
import time
import random
import os
import re
from urllib.parse import urljoin

# 헤더 설정 함수
def get_headers():
    user_agents = [
        # 최신 브라우저 User-Agent로 변경
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15"
    ]
    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
        "Connection": "keep-alive",
        "Referer": "https://www.instiz.net/",
        "Upgrade-Insecure-Requests": "1"
    }
    return headers


# URL에서 게시글 ID 추출 함수
def extract_post_id(url):
    match = re.search(r'/(\d+)', url)
    if match:
        return match.group(1)
    return None

# 날짜 문자열 파싱 함수
def parse_date_str(date_str, today):
    print(f"날짜 파싱 시도: '{date_str}'", flush=True)
    if not date_str:
        print("날짜 문자열이 비어있음")
        return None
        
    if len(date_str.split()) == 1:  # 시간만 있는 경우 (예: "8:10")
        return today
    else:
        try:
            # "03.18 21:58" 형식 처리
            date_part = date_str.split()[0]  # "03.18" 추출
            if '.' in date_part:
                month, day = map(int, date_part.split('.'))
                year = today.year
                
                # 월과 일로 날짜 생성
                parsed_date = datetime(year, month, day).date()
                
                # 연도가 바뀌는 경계 처리 (12월->1월)
                if month == 12 and today.month == 1:
                    parsed_date = datetime(year - 1, month, day).date()
                elif month == 1 and today.month == 12:
                    parsed_date = datetime(year + 1, month, day).date()
                
                print(f"날짜 파싱 결과: '{date_str}' -> {parsed_date}")
                return parsed_date
            elif '-' in date_part:
                # "2023-03-18 21:58" 형식 처리
                parsed_date = datetime.strptime(date_part, '%Y-%m-%d').date()
                print(f"날짜 파싱 결과: '{date_str}' -> {parsed_date}", flush=True)
                return parsed_date
            else:
                print(f"알 수 없는 날짜 형식: '{date_str}'")
                return None
        except (ValueError, IndexError) as e:
            print(f"날짜 파싱 오류: '{date_str}' - {e}")
            return None  # 파싱 실패 시 None 반환

# 게시글 내용 크롤링
def get_post_content(post_url):
    try:
        headers = get_headers()
        response = requests.get(post_url, headers=headers, timeout=10)
        print(f"[DEBUG] 응답 헤더: {response.headers}")
        if response.status_code != 200:
            print(f"[ERROR] 비정상 응답 본문:\n{response.text[:500]}...")
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = Soup(response.text, "html.parser")

        content_div = soup.find("div", class_="memo_content")
        if not content_div:
            print(f"내용 영역을 찾을 수 없습니다: {post_url}")
            return {"text": "", "images": []}

        text_content = content_div.get_text(separator="\n", strip=True)
        image_urls = []
        for img in content_div.find_all("img"):
            src = img.get("src")
            if src and "instiz.net/images/ico_loading.gif" not in src:
                if src.startswith("//"):
                    src = f"https:{src}"
                elif not src.startswith("http"):
                    src = f"https://www.instiz.net{src}"
                image_urls.append(src)

        return {"text": text_content, "images": image_urls}
    except Exception as e:
        print(f"게시글 크롤링 실패: {post_url} - {e}")
        return {"text": "", "images": []}

# 게시글 정보 파싱 함수
def get_post_info(post):
    try:
        # 카테고리 찾기
        category_elem = post.find("span", class_="list_category")
        category = category_elem.text.strip() if category_elem else ""
        
        # 제목 찾기
        subject_elem = post.find("td", class_=re.compile("listsubject"))
        if not subject_elem:
            return None
            
        link_elem = subject_elem.find("a")
        link = urljoin("https://www.instiz.net", link_elem['href']) if link_elem else ""
        post_id = extract_post_id(link)
        
        # 제목 텍스트 추출
        title_elem = link_elem.find("span", class_="texthead_notice") if link_elem else None
        title = title_elem.text.strip() if title_elem else (link_elem.text.strip() if link_elem else "")
        
        # 작성자 찾기
        writer_elem = post.find("td", class_=re.compile("listnm")).find("a")
        writer = writer_elem.text.strip() if writer_elem else ""
        
        # 날짜 찾기 - width 속성으로 찾기
        date_elem = post.find("td", class_="listno", width="60")
        if not date_elem:
            date_elem = post.find("td", class_="listno regdate")
        date_str = date_elem.text.strip() if date_elem else ""
        
        # 조회수 찾기
        views_elem = post.find("td", class_="listno", width="45")
        if not views_elem:
            views_elems = post.find_all("td", class_="listno")
            views_elem = views_elems[1] if len(views_elems) > 1 else None
        
        views = 0
        if views_elem:
            try:
                views = int(views_elem.text.strip().replace(',', ''))
            except ValueError:
                print(f"조회수 파싱 오류: {views_elem.text.strip()}")
        
        # 추천수 찾기
        recommend_elem = post.find("td", class_="listno", width="25")
        if not recommend_elem:
            recommend_elems = post.find_all("td", class_="listno")
            recommend_elem = recommend_elems[2] if len(recommend_elems) > 2 else None
        
        recommend = 0
        if recommend_elem:
            try:
                recommend = int(recommend_elem.text.strip())
            except ValueError:
                print(f"추천수 파싱 오류: {recommend_elem.text.strip()}")
        
        # 댓글 수 파싱
        comment_elem = None
        if link_elem:
            comment_elem = link_elem.find("span", class_=re.compile("cmt[2-3]"))
        comment_count = 0
        if comment_elem:
            try:
                comment_count = int(comment_elem.text.strip())
            except ValueError:
                print(f"댓글 수 파싱 오류: {comment_elem.text.strip()}")
        
        return {
            "post_id": post_id,
            "category": category,
            "title": title,
            "link": link,
            "writer": writer,
            "date_str": date_str,
            "views": views,
            "recommend": recommend,
            "comments": comment_count
        }
    except Exception as e:
        print(f"게시글 정보 파싱 오류: {e}")
        return None

# 인스티즈 게시판 크롤링 메인 함수
def instiz_pt_crawl(min_views=1000):
    base_url = 'https://www.instiz.net/pt'
    today = datetime.now().date()
    data = []
    page = 1
    no_today_count = 0
    max_no_today = 3
    max_retries = 3
    total_posts_collected = 0

    print(f"크롤링 시작 - 오늘 날짜: {today}, 최소 조회수: {min_views}")

    while no_today_count < max_no_today:
        if page > 15:  # 페이지 번호 20을 넘으면 크롤링 종료
            break
        page_url = base_url if page == 1 else f"{base_url}?page={page}"
        print(f"\n페이지 {page} 크롤링 중: {page_url}", flush=True)

        retry_count = 0
        while retry_count < max_retries:
            try:
                headers = get_headers()
                response = requests.get(page_url, headers=headers, timeout=10)
                response.raise_for_status()
                response.encoding = 'utf-8'
                print(f"응답 상태 코드: {response.status_code}, 응답 길이: {len(response.text)} 바이트")

                soup = Soup(response.text, "html.parser")
                if len(response.text) < 1000:
                    print(f"비정상 응답 감지: {response.text[:500]}")
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"재시도 {retry_count}/{max_retries}...")
                        time.sleep(random.uniform(2, 5))
                        continue
                    else:
                        no_today_count += 1
                        break
                
                print("HTML 파싱 완료", flush=True)
                all_detours = []
                
                # mboard 테이블에서 detour 찾기
                mboard_table = soup.find("table", id="mboard", class_="mboard")
                if mboard_table:
                    detours = mboard_table.find_all("tr", id="detour", recursive=True)
                    all_detours.extend(detours)
                    print(f"mboard에서 찾은 detour 개수: {len(detours)}", flush=True)
                else:
                    print("mboard 테이블을 찾을 수 없습니다")

                # green_mainboard 테이블에서 detour 찾기
                green_tables = soup.find_all("table", id=re.compile("green_mainboard[0-2]?"))
                print(f"green_mainboard 테이블 개수: {len(green_tables)}", flush=True)
                for green_table in green_tables:
                    green_detours = green_table.find_all("tr", id="detour")
                    all_detours.extend(green_detours)
                    print(f"{green_table.get('id')}에서 찾은 detour 개수: {len(green_detours)}")

                if not all_detours:
                    print("id='detour'를 가진 게시물이 없습니다.")
                    no_today_count += 1
                    break

                print(f"총 detour 게시글 개수: {len(all_detours)}", flush=True)
                has_today_post = False
                page_posts_collected = 0
                
                for post in all_detours:
                    post_info = get_post_info(post)
                    if not post_info:
                        print("게시글 정보 파싱 실패, 다음 게시글로 넘어갑니다.")
                        continue
                    
                    # 날짜 처리
                    date_str = post_info["date_str"]
                    post_date = parse_date_str(date_str, today)
                    
                    if post_date is None:
                        print("날짜 파싱 실패, 다음 게시글로 넘어갑니다.")
                        continue
                        
                    if post_date == today:
                        has_today_post = True
                        print(f"오늘 날짜 게시글 발견: {post_info['title']} (조회수: {post_info['views']})", flush=True)
                        
                        # 조회수 필터링
                        if post_info["views"] < min_views:
                            print(f"조회수 부족 ({post_info['views']} < {min_views}), 건너뜁니다.")
                            continue
                        
                        # 내용 크롤링
                        print(f"게시글 내용 크롤링 중: {post_info['link']}", flush=True)
                        content_data = get_post_content(post_info["link"])
                        
                        # 데이터 추가
                        data.append({
                            "Post ID": post_info["post_id"],
                            "Community": "3",
                            "Category": post_info["category"],
                            "Title": post_info["title"],
                            "Link": post_info["link"],
                            "Writer": post_info["writer"],
                            "Date": date_str,
                            "Recommend": post_info["recommend"],
                            "Views": post_info["views"],
                            "Comments": post_info["comments"],
                            "Content": content_data["text"],
                            "Images": content_data["images"]
                        })
                        
                        page_posts_collected += 1
                        total_posts_collected += 1
                        print(f"게시물 수집 완료: {post_info['title']} (ID: {post_info['post_id']}, Views: {post_info['views']})", flush=True)
                    else:
                        print(f"오늘 날짜가 아닌 게시글: {post_info['title']} (날짜: {post_date})", flush=True)
                
                print(f"이 페이지에서 수집한 게시글 수: {page_posts_collected}")
                print(f"지금까지 총 수집한 게시글 수: {total_posts_collected}")
                
                if has_today_post:
                    no_today_count = 0  # 오늘 날짜 게시글이 하나라도 있으면 카운터 초기화
                    print(f"오늘 날짜 게시글을 찾았습니다. 연속 카운터 초기화: {no_today_count}", flush=True)
                else:
                    no_today_count += 1
                    print(f"오늘 날짜 게시글 없음. 연속 카운트: {no_today_count}/{max_no_today}", flush=True)
                
                if no_today_count >= max_no_today:
                    print(f"오늘 날짜 게시글이 없는 페이지가 {max_no_today}번 연속으로 나와 크롤링을 종료합니다.")
                    break
                
                page += 1
                time.sleep(random.uniform(2, 5))
                break  # 성공적으로 처리했으므로 재시도 루프 종료
                
            except Exception as e:
                print(f"페이지 로드 오류: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    print(f"재시도 {retry_count}/{max_retries}...")
                    time.sleep(random.uniform(3, 7))
                else:
                    no_today_count += 1
                    page += 1
                    break

    print(f"크롤링 종료 - 총 수집한 게시글 수: {total_posts_collected}")
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
    
    df = instiz_pt_crawl(min_views=1000)
    if df is not None and not df.empty:
        available_cols = [col for col in ["Post_ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Comments", "Content", "Images"] if col in df.columns]
        print(f"수집된 데이터 미리보기:")
        print(df[available_cols].head())
        print(f"총 수집된 게시글 수: {len(df)}")
        
        file_name = f"instiz_issue_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 '{file_path}' 파일로 저장되었습니다.")
    else:
        print("수집된 데이터가 없습니다.")
