import aiohttp
import asyncio
import json
from typing import List, Dict, Any

class StockNewsAnalyzer:
    def __init__(self, config):
        self.config = config['ai_analysis']
        self.api_key = self.config['api_key']
        self.base_url = self.config['base_url']

    async def analyze_stock_news(self, stock_info: Dict, news_list: List[str]) -> Dict:
        """核心方法：分析单只股票的新闻和涨停原因"""
        prompt = self._build_analysis_prompt(stock_info, news_list)
        analysis_result = await self._call_deepseek_api(prompt)
        return self._parse_analysis_result(analysis_result)

    def _build_analysis_prompt(self, stock_info: Dict, news_list: List[str]) -> str:
        # 构建一个专业的分析提示词
        prompt = f"""你是一名资深A股分析师，请分析以下股票涨停的原因。

        股票信息：
        - 名称：{stock_info.get('name')}
        - 代码：{stock_info.get('code')}
        - 角色：{stock_info.get('role')}
        - 连板天数：{stock_info.get('limit_up_days')}
        - 所属板块：{', '.join(stock_info.get('sectors', []))}

        相关新闻与信息（{len(news_list)}条）：
        {chr(10).join([f'{i+1}. {news}' for i, news in enumerate(news_list)])}

        请从以下维度进行结构化分析：
        1. **直接消息催化**：哪些具体消息直接导致了涨停？
        2. **基本面支撑**：业绩、估值、行业地位等因素。
        3. **技术面分析**：资金流向、技术形态、市场情绪。
        4. **板块效应**：所属板块整体表现如何？
        5. **持续性判断**：涨停势头是否可持续？风险提示。

        要求：分析具体、有数据支撑、指出核心驱动因素。"""
        return prompt