import arxiv
import yaml
import time
from urllib.parse import quote
from typing import List, Dict

# 配置项
BATCH_SIZE = 5  # 每批次查询的关键词数量（避免URL过长）
RETRY_TIMES = 3  # 重试次数
RETRY_DELAY = 2  # 重试间隔（秒）

def load_topics(yaml_path: str) -> List[Dict]:
    """加载topics配置"""
    with open(yaml_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    return data.get('profiles', [])

def build_search_query(keywords: List[str]) -> str:
    """构建单个批次的搜索查询（正确逻辑：任意关键词匹配title/abstract）"""
    clauses = []
    for kw in keywords:
        # 每个关键词：(title:关键词 OR abstract:关键词)
        escaped_kw = quote(kw)  # 安全编码特殊字符
        clause = f"(title:{escaped_kw} OR abstract:{escaped_kw})"
        clauses.append(clause)
    # 多个关键词用OR连接（匹配任意一个即可）
    return " OR ".join(clauses)

def search_arxiv_batch(keywords_batch: List[str], exclude_terms: List[str]) -> List[arxiv.Result]:
    """单批次搜索arXiv，带重试机制"""
    query = build_search_query(keywords_batch)
    
    # 添加排除词（title/abstract中不含排除词）
    if exclude_terms:
        exclude_clauses = [f"NOT (title:{term} OR abstract:{term})" for term in exclude_terms]
        query += " " + " ".join(exclude_clauses)
    
    # 配置搜索参数
    search = arxiv.Search(
        query=query,
        max_results=100,
        sort_by=arxiv.SortCriterion.SubmittedDate,
        sort_order=arxiv.SortOrder.Descending
    )
    
    # 重试逻辑
    for retry in range(RETRY_TIMES):
        try:
            client = arxiv.Client(
                page_size=100,
                delay_seconds=1,  # 避免请求过快
                num_retries=1
            )
            results = list(client.results(search))
            return results
        except arxiv.HTTPError as e:
            if retry < RETRY_TIMES - 1:
                print(f"请求失败（重试{retry+1}/{RETRY_TIMES}）：{e}")
                time.sleep(RETRY_DELAY * (retry + 1))  # 指数退避
                continue
            else:
                raise e

def split_keywords_to_batches(keywords: List[str], batch_size: int) -> List[List[str]]:
    """拆分关键词为多个批次"""
    batches = []
    for i in range(0, len(keywords), batch_size):
        batch = keywords[i:i+batch_size]
        batches.append(batch)
    return batches

def fetch_papers_by_topic():
    """按主题批量获取论文"""
    topics = load_topics("topics.yml")
    print(f"开始读取topics.yml的筛选规则，共 {len(topics)} 个主题")
    
    all_results = []
    for topic in topics:
        name = topic.get('name')
        include_kw = topic.get('include', [])
        exclude_kw = topic.get('exclude', [])
        
        print(f"\n处理主题：{name}（关键词数量：{len(include_kw)}）")
        
        # 拆分关键词为批次
        keyword_batches = split_keywords_to_batches(include_kw, BATCH_SIZE)
        topic_results = []
        
        # 逐批次查询
        for idx, batch in enumerate(keyword_batches):
            print(f"  处理批次 {idx+1}/{len(keyword_batches)}（关键词：{batch}）")
            batch_results = search_arxiv_batch(batch, exclude_kw)
            topic_results.extend(batch_results)
            time.sleep(1)  # 批次间间隔，避免压测服务器
        
        # 去重（按论文ID）
        unique_results = []
        seen_ids = set()
        for res in topic_results:
            if res.entry_id not in seen_ids:
                seen_ids.add(res.entry_id)
                unique_results.append(res)
        
        print(f"  主题{name}最终获取到 {len(unique_results)} 篇唯一论文")
        all_results.extend(unique_results)
    
    # 后续处理（保存/输出等）
    print(f"\n总计获取到 {len(all_results)} 篇论文")
    # TODO: 保存结果到文件/数据库

if __name__ == "__main__":
    fetch_papers_by_topic()