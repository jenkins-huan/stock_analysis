"""
板块效应分析模块
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import logging
from collections import defaultdict

class SectorAnalyzer:
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def analyze_sectors(self, limit_up_df: pd.DataFrame, 
                       historical_data: Dict[str, pd.DataFrame]) -> Dict[str, List]:
        """分析板块效应"""
        if limit_up_df.empty:
            return {}
            
        # 按板块分组
        sector_stocks = self._group_stocks_by_sector(limit_up_df)
        
        analysis_results = {}
        
        for sector, stocks in sector_stocks.items():
            if len(stocks) >= 2:  # 板块至少有2只涨停
                sector_analysis = self._analyze_single_sector(sector, stocks, historical_data)
                analysis_results[sector] = sector_analysis
                
        # 按板块强度排序
        sorted_sectors = sorted(analysis_results.items(), 
                              key=lambda x: x[1]['强度得分'], 
                              reverse=True)
        
        return dict(sorted_sectors)
    
    def _group_stocks_by_sector(self, df: pd.DataFrame) -> Dict[str, List]:
        """按板块分组股票（简化版）"""
        sectors = {
            '科技': ['半导体', '软件', '通信', '人工智能', '芯片', '5G'],
            '新能源': ['光伏', '锂电池', '风电', '储能', '新能源汽车', '氢能源'],
            '医药': ['中药', '生物制药', '医疗器械', '医疗服务', '医药商业'],
            '消费': ['白酒', '食品饮料', '家电', '零售', '旅游', '酒店'],
            '周期': ['化工', '有色', '煤炭', '钢铁', '建材', '化纤'],
            '金融': ['银行', '证券', '保险', '信托', '金融科技'],
            '其他': ['其他']
        }

        sector_stocks = defaultdict(list)

        for _, row in df.iterrows():
            code = row['code']
            sector = self._assign_stock_sector(code, sectors)
            sector_stocks[sector].append(row.to_dict())

        return dict(sector_stocks)

    def _assign_stock_sector(self, code: str, sectors: Dict) -> str:
        """分配股票到板块（简化）"""
        sector_keys = list(sectors.keys())
        sector_index = hash(code) % len(sector_keys)
        return sector_keys[sector_index]

    def _analyze_single_sector(self, sector: str, stocks: List[Dict],
                              historical_data: Dict) -> Dict:
        """分析单个板块"""
        analysis = {
            '板块名称': sector,
            '涨停数量': len(stocks),
            '强度得分': 0,
            '梯队完整性': '',
            '资金流入': 0,
            '持续性判断': '',
            '核心股票': []
        }

        # 计算板块强度得分
        strength_score = self._calculate_sector_strength(stocks)
        analysis['强度得分'] = strength_score

        # 分析梯队
        analysis['梯队完整性'] = self._check_sector_structure(stocks)

        # 计算资金流入
        analysis['资金流入'] = sum(s.get('amount', 0) for s in stocks)

        # 判断持续性
        analysis['持续性判断'] = self._judge_sector_persistence(stocks, strength_score)

        # 识别核心股票
        analysis['核心股票'] = self._identify_core_stocks(stocks)

        return analysis

    def _calculate_sector_strength(self, stocks: List[Dict]) -> float:
        """计算板块强度得分"""
        if not stocks:
            return 0.0

        score = 0.0

        # 1. 涨停数量得分
        count_score = min(len(stocks) * 10, 50)

        # 2. 连板高度得分
        max_continuous = max(s.get('continuous_days', 0) for s in stocks)
        continuous_score = min(max_continuous * 15, 30)

        # 3. 成交额得分
        total_amount = sum(s.get('amount', 0) for s in stocks)
        if total_amount > 5e9:  # 50亿以上
            amount_score = 20
        elif total_amount > 1e9:  # 10亿以上
            amount_score = 15
        elif total_amount > 5e8:  # 5亿以上
            amount_score = 10
        else:
            amount_score = 5

        score = count_score + continuous_score + amount_score
        return round(score, 2)

    def _check_sector_structure(self, stocks: List[Dict]) -> str:
        """检查板块梯队完整性"""
        heights = [s.get('continuous_days', 0) for s in stocks]
        unique_heights = set(heights)
        
        if len(unique_heights) >= 3:
            return "完整（多梯队）"
        elif len(unique_heights) == 2:
            return "一般（双梯队）"
        else:
            return "单一（单梯队）"
    
    def _judge_sector_persistence(self, stocks: List[Dict], strength_score: float) -> str:
        """判断板块持续性"""
        if strength_score >= 70:
            return "强势，有望持续"
        elif strength_score >= 50:
            return "中等，可能分化"
        elif strength_score >= 30:
            return "一般，谨慎参与"
        else:
            return "弱势，可能一日游"
    
    def _identify_core_stocks(self, stocks: List[Dict]) -> List[Dict]:
        """识别板块核心股票"""
        if not stocks:
            return []
            
        # 按连板高度排序
        sorted_stocks = sorted(stocks, key=lambda x: x.get('continuous_days', 0), reverse=True)
        
        core_stocks = []
        for stock in sorted_stocks[:3]:  # 取前三
            core_stock = {
                'code': stock.get('code'),
                'name': stock.get('name', stock.get('code')),
                'role': self._assign_stock_role(stock, sorted_stocks),
                'continuous_days': stock.get('continuous_days', 0),
                'amount': stock.get('amount', 0)
            }
            core_stocks.append(core_stock)
            
        return core_stocks
    
    def _assign_stock_role(self, stock: Dict, all_stocks: List[Dict]) -> str:
        """分配股票在板块中的角色"""
        continuous_days = stock.get('continuous_days', 0)
        
        if continuous_days >= 3:
            return "高度龙头"
        elif continuous_days == 2:
            return "跟随龙"
        else:
            # 检查是否是中军（成交额大）
            amounts = [s.get('amount', 0) for s in all_stocks]
            if stock.get('amount', 0) >= max(amounts) * 0.7:
                return "趋势中军"
            else:
                return "补涨/跟风"