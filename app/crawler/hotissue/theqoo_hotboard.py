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
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://theqoo.net/",
        "Upgrade-Insecure-Requests": "1"
    }

# 게시글 내용 크롤링 (정적 방식)
def get_post_content(post_url):
    try:
        headers = get_headers()
        response = requests.get(post_url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = Soup(response.text, "html.parser")

        content_div = soup.find("div", class_="rd_body clear") or soup.find("article", itemprop="articleBody")
        if not content_div:
            print(f"내용 영역을 찾을 수 없습니다: {post_url}")
            return {"text": "", "images": []}

        text_content = content_div.get_text(separator="\n", strip=True)

        image_urls = []
        for img in content_div.find_all("img"):
            src = img.get("src")
            if src:
                if src.startswith("//"):
                    src = f"https:{src}"
                image_urls.append(src)

        return {"text": text_content, "images": image_urls}
    except Exception as e:
        print(f"게시글 크롤링 실패: {post_url} - {e}")
        return {"text": "", "images": []}

# 더쿠 핫 게시판 크롤링 메인 함수
def theqoo_hotboard_crawl(min_views=10000, max_page=3):
    base_url = 'https://theqoo.net/hot?filter_mode=normal'
    today = datetime.now().date()
    data = []

    for page in range(1, max_page + 1):
        page_url = f"{base_url}&page={page}"
        print(f"페이지 {page} 크롤링 중: {page_url}")

        try:
            headers = get_headers()
            response = requests.get(page_url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = 'utf-8'
            soup = Soup(response.text, "html.parser")

            # 디버깅: HTML 일부 출력
            print(f"페이지 HTML (처음 1000자): {soup.prettify()[:1000]}")

            # 테이블 찾기
            board_table = soup.find("table", class_="bd_lst bd_tb_lst bd_tb theqoo_board_table")
            if not board_table:
                print("게시판 테이블을 찾을 수 없습니다.")
                continue

            # tbody 찾기
            tbody = board_table.find("tbody", class_="hide_notice")
            if not tbody:
                print("tbody를 찾을 수 없습니다.")
                continue

            # 모든 <tr> 요소 가져오기
            posts = tbody.find_all("tr")
            print(f"페이지 {page}에서 찾은 게시글 수: {len(posts)}")

            for post in posts:
                # 공지사항 필터링
                post_classes = post.get("class", [])
                if "notice" in post_classes or "nofn" in post_classes:
                    print(f"공지사항 제외: {post.get_text(strip=True)[:50]}...")
                    continue

                # 날짜
                date_elem = post.find("td", class_="time")
                date_str = date_elem.text.strip() if date_elem else ""
                print(f"게시글 날짜: {date_str}")
                if ":" not in date_str:
                    print(f"오늘 날짜 아님, 제외됨: {date_str}")
                    continue

                # 조회수
                views_elem = post.find("td", class_="m_no")
                views_text = views_elem.text.strip().replace(',', '') if views_elem else "0"
                views = int(views_text) if views_text.isdigit() else 0
                print(f"게시글 조회수: {views}")
                if views < min_views:
                    print(f"조회수 {views} < {min_views}, 제외됨")
                    continue

                # 제목과 링크
                title_td = post.find("td", class_="title")
                if not title_td:
                    print("제목 td 요소를 찾을 수 없음, 제외됨")
                    continue
                title_elem = title_td.find("a")  # 첫 번째 <a> 태그 찾기
                if not title_elem:
                    print("제목 a 요소를 찾을 수 없음, 제외됨")
                    continue
                title = title_elem.text.strip()
                link = title_elem["href"]
                if not link.startswith('http'):
                    link = f"https://theqoo.net{link}"
                print(f"게시글 제목: {title}, 링크: {link}")

                # 게시글 ID
                post_elem = post.find("td", class_="no")
                post_id = post_elem.text.strip() if post_elem else ""
                print(f"게시글 ID: {post_id}")

                # 카테고리
                category_elem = post.find("td", class_="cate")
                category = category_elem.text.strip() if category_elem else ""
                print(f"게시글 카테고리: {category}")

                # 작성자
                writer = "무명의 더쿠"

                # 댓글 수
                reply_elem = post.find("a", class_="replyNum")
                reply_count = int(reply_elem.text.strip()) if reply_elem and reply_elem.text.strip().isdigit() else 0
                print(f"게시글 댓글 수: {reply_count}")

                # 게시글 내용 크롤링
                content_data = get_post_content(link)

                data.append({
                    "Post ID": post_id,
                    "Community": "2",
                    "Category": category,
                    "Title": title,
                    "Link": link,
                    "Writer": writer,
                    "Date": f"{today} {date_str}",
                    "Recommend": reply_count,
                    "Views": str(views),
                    "Content": content_data["text"],
                    "Images": content_data["images"]
                })

                print(f"게시물 수집 완료: {title} (ID: {post_id})")

            time.sleep(random.uniform(5, 10))  # 요청 간격 늘림

        except Exception as e:
            print(f"페이지 로드 오류: {e}")
            continue

    df = pd.DataFrame(data)
    print(f"총 수집된 게시글 수: {len(data)}")
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
    
    df = theqoo_hotboard_crawl(min_views=7000, max_page=3)
    if df is not None:
        available_cols = [col for col in ["Post ID", "Category", "Title", "Writer", "Date", "Views", "Recommend", "Content", "Images"] if col in df.columns]
        print(df[available_cols])
        file_name = f"theqoo_hotboard_{today}.csv"
        file_path = os.path.join(today_folder, file_name)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 '{file_path}' 파일로 저장되었습니다.")
    else:
        print("크롤링된 데이터가 없습니다.")