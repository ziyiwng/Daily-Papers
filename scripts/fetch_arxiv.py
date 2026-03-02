import yaml
import arxiv
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

# 解析项目根目录，确保无论从哪里运行都写入仓库内。
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
TOPICS_PATH = BASE_DIR / "topics.yml"

# 读取根目录的topics.yml配置文件
with open(TOPICS_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# 创建data目录（自动生成）
os.makedirs(DATA_DIR, exist_ok=True)
# 汇总文件路径（保留所有论文）
ALL_OUTPUT_PATH = DATA_DIR / "arxiv_all.json"

def fetch_papers_by_topic():
    all_papers = []
    print(f"开始读取topics.yml的筛选规则，共 {len(config['profiles'])} 个主题")
    # 使用新版 Client.results() 接口，避免 Search.results() 的弃用警告。
    client = arxiv.Client(
        page_size=10,  # 减小分页大小，降低单次请求压力
        delay_seconds=2,  # 添加请求延迟，避免频繁请求触发限制
        num_retries=3  # 重试次数
    )
    
    for profile in config['profiles']:
        topic_name = profile['name']
        include_keywords = profile.get('include', [])
        exclude_keywords = profile.get('exclude', [])
        categories = profile.get('categories', [])
        max_results = profile.get('max', 10)
        
        # ========== 核心改动1：重构查询逻辑 ==========
        # 1. 分类条件（如果有）
        cat_terms = [f"cat:{cat}" for cat in categories] if categories else []
        # 2. 关键词条件：所有include关键词用OR连接（找包含任意一个的论文）
        kw_terms = []
        for kw in include_keywords:
            # 对含空格/特殊字符的关键词加引号，避免解析错误
            quoted_kw = f'"{kw}"' if ' ' in kw else kw
            kw_terms.append(f"(title:{quoted_kw} OR abstract:{quoted_kw})")
        
        # 组合查询：分类 AND (关键词1 OR 关键词2 OR ...)
        query_parts = []
        if cat_terms:
            query_parts.append(" AND ".join(cat_terms))
        if kw_terms:
            query_parts.append(" OR ".join(kw_terms))
        
        # 最终查询：无条件则查全部
        query = " AND ".join(query_parts) if query_parts else "*"
        # 限制查询长度（ArXiv API建议URL不超过2000字符）
        if len(query) > 1500:
            print(f"⚠️ {topic_name} 关键词过多，查询语句过长，仅保留前10个关键词")
            kw_terms_truncated = kw_terms[:10]
            query_parts = [cat_terms[0]] if cat_terms else []
            query_parts.append(" OR ".join(kw_terms_truncated))
            query = " AND ".join(query_parts) if query_parts else "*"

        # ========== 核心改动2：添加异常处理 ==========
        try:
            # 调用arxiv接口爬取论文（最新提交优先）
            search = arxiv.Search(
                query=query,
                max_results=max_results,
                sort_by=arxiv.SortCriterion.SubmittedDate,
                sort_order=arxiv.SortOrder.Descending
            )

            topic_papers = []
            # 筛选论文：排除指定关键词
            for result in client.results(search):
                title_abs = f"{result.title} {result.summary}".lower()
                exclude_flag = False
                for ek in exclude_keywords:
                    if ek.lower() in title_abs:
                        exclude_flag = True
                        break
                if exclude_flag:
                    continue
                
                # 整理论文数据
                paper = {
                    "title": result.title,
                    "authors": [author.name for author in result.authors],
                    "arxiv_id": result.get_short_id(),
                    "url": result.entry_id,
                    "summary": result.summary.strip().replace("\n", " "),
                    "published": result.published.strftime("%Y-%m-%d %H:%M"),
                    "updated": result.updated.strftime("%Y-%m-%d %H:%M"),
                    "category": topic_name,
                    "categories": result.categories
                }
                topic_papers.append(paper)
                all_papers.append(paper)
            
            # 为每个主题生成独立的json文件
            topic_json_name = f"arxiv_{topic_name.replace(' ','_')}.json"
            topic_json_path = DATA_DIR / topic_json_name
            with open(topic_json_path, "w", encoding="utf-8") as f:
                json.dump(topic_papers, f, ensure_ascii=False, indent=2)
            print(f"✅ {topic_name} 主题爬取完成，共 {len(topic_papers)} 篇，保存至 {topic_json_name}")
        
        except arxiv.HTTPError as e:
            print(f"❌ {topic_name} 爬取失败：ArXiv API 错误 {e.status_code}")
            print(f"   错误详情：{e.url}")
            continue  # 跳过当前主题，继续下一个
        except Exception as e:
            print(f"❌ {topic_name} 爬取失败：{str(e)}")
            continue
    
    # 去重 + 按更新时间排序，保存汇总文件
    unique_papers = list({p['arxiv_id']: p for p in all_papers}.values())
    unique_papers.sort(key=lambda x: x['updated'], reverse=True)
    
    with open(ALL_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(unique_papers, f, ensure_ascii=False, indent=2)
    
    print(f"\n📊 全部主题爬取完成！共获取 {len(unique_papers)} 篇论文，汇总保存至 arxiv_all.json")
    return unique_papers

if __name__ == "__main__":
    fetch_papers_by_topic()