"""
龙头、中军、补涨识别模块
使用多因子评分系统
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import logging

class DragonHeadIdentifier:
    def __init__(self, config: Dict):
        self.config = config
        self.weights = config['analysis']['龙头评分权重']
        self.logger = logging.getLogger(__name__)
        
    def identify_roles(self, limit_up_stocks: List[Dict], 
                      sector_data: Dict[str, List]) -> Dict[str, List]:
        """识别龙头、中军、补涨"""
        if not limit_up_stocks:
            return self._empty_roles()

        self.logger.info(f"👑 开始识别龙头/中军/补涨，共 {len(limit_up_stocks)} 只股票")

        # 按板块分组
        sector_groups = self._group_by_sector(limit_up_stocks, sector_data)

        roles = {
            '龙头': [],
            '中军': [],
            '补涨': [],
            '观察': []
        }

        # 分析每个板块
        for sector, stocks in sector_groups.items():
            if len(stocks) >= self.config['analysis']['板块强度阈值']:
                self.logger.info(f"  分析板块: {sector} ({len(stocks)}只涨停)")
                sector_roles = self._analyze_sector_roles(sector, stocks)

                for role_type, stock in sector_roles.items():
                    if stock:
                        stock['所属板块'] = sector
                        roles[role_type].append(stock)
            else:
                # 板块强度不够的股票放入观察列表
                for stock in stocks:
                    stock['所属板块'] = sector
                    roles['观察'].append(stock)

        # 按评分排序
        for role_type in ['龙头', '中军', '补涨']:
            if roles[role_type]:
                roles[role_type].sort(key=lambda x: x.get('综合评分', 0), reverse=True)
                self.logger.info(f"  {role_type}: {len(roles[role_type])}只")

        self.logger.info(f"✅ 角色识别完成")
        return roles

    def _group_by_sector(self, stocks: List[Dict], sector_data: Dict) -> Dict[str, List]:
        """按板块分组"""
        sector_groups = {}

        for stock in stocks:
            code = stock['code']
            # 从sector_data获取板块信息，如果没有则使用默认方法
            sector = self._get_stock_sector_from_data(code, sector_data)

            if sector not in sector_groups:
                sector_groups[sector] = []
            sector_groups[sector].append(stock)

        return sector_groups

    def _get_stock_sector_from_data(self, code: str, sector_data: Dict) -> str:
        """从板块数据获取股票所属板块"""
        # 首先尝试从sector_data中查找
        for sector, sector_info in sector_data.items():
            core_stocks = sector_info.get('核心股票', [])
            for core_stock in core_stocks:
                if core_stock.get('code') == code:
                    return sector

        # 如果没有找到，使用简化的板块分配
        return self._get_stock_sector(code)

    def _get_stock_sector(self, code: str) -> str:
        """获取股票所属板块（简化版）"""
        # 根据股票代码前缀判断（这是一个简化的方法）
        # 实际应用中应该使用股票的基本信息或专门的板块数据

        sectors = {
            '6': ['银行', '证券', '保险', '基建', '能源'],  # 沪市主板
            '0': ['中小板', '制造业', '科技'],  # 深市主板
            '3': ['创业板', '科技', '医药', '新能源'],  # 创业板
            '4': ['科创板', '高科技', '半导体', '生物医药']  # 科创板
        }

        if code[0] in sectors:
            sector_list = sectors[code[0]]
            # 使用哈希决定具体板块
            sector_index = hash(code) % len(sector_list)
            return sector_list[sector_index]

        return '其他'

    def _analyze_sector_roles(self, sector: str, stocks: List[Dict]) -> Dict[str, Dict]:
        """分析板块内各角色"""
        if len(stocks) < 2:
            return {}

        # 计算每只股票的评分
        scored_stocks = []
        for stock in stocks:
            score = self._calculate_dragon_score(stock)
            stock['综合评分'] = score
            scored_stocks.append(stock)

        # 按评分排序
        scored_stocks.sort(key=lambda x: x['综合评分'], reverse=True)

        roles = {}

        # 识别龙头（评分最高，连板最多）
        if len(scored_stocks) > 0:
            # 龙头应该具有最高的连板高度
            dragon_candidates = [s for s in scored_stocks if s.get('continuous_days', 0) >= 2]
            if dragon_candidates:
                roles['龙头'] = max(dragon_candidates, key=lambda x: x['综合评分'])
            else:
                roles['龙头'] = scored_stocks[0]

        # 识别中军（成交额最大，市值较大）
        if len(scored_stocks) > 1:
            # 中军通常成交额大，但连板可能不多
            middle_army_candidates = scored_stocks[:min(5, len(scored_stocks))]
            middle_army = max(middle_army_candidates, key=lambda x: x.get('amount', 0))

            # 确保中军不是龙头
            if '龙头' in roles and roles['龙头']['code'] == middle_army['code']:
                # 如果中军和龙头是同一只，取下一个
                if len(scored_stocks) > 2:
                    middle_army_candidates = [s for s in scored_stocks[1:3]
                                             if s['code'] != roles['龙头']['code']]
                    if middle_army_candidates:
                        middle_army = max(middle_army_candidates, key=lambda x: x.get('amount', 0))

            roles['中军'] = middle_army

        # 识别补涨（价格位置低，首次涨停或1连板）
        if len(scored_stocks) > 2:
            fill_up_candidates = [s for s in scored_stocks
                                if s.get('continuous_days', 0) <= 1 and
                                s.get('features', {}).get('price_position', 50) < 50]

            if not fill_up_candidates:
                # 如果没有低位的，选择评分较低但技术形态好的
                fill_up_candidates = [s for s in scored_stocks[-3:]
                                    if s.get('features', {}).get('is_breakout', False)]

            if fill_up_candidates:
                # 选择技术形态最好的
                fill_up = max(fill_up_candidates,
                            key=lambda x: x.get('features', {}).get('trend_strength', 0))

                # 确保不是龙头或中军
                if ('龙头' in roles and fill_up['code'] != roles['龙头']['code'] and
                    '中军' in roles and fill_up['code'] != roles['中军']['code']):
                    roles['补涨'] = fill_up

        return roles

    def _calculate_dragon_score(self, stock: Dict) -> float:
        """计算龙头评分"""
        score = 0.0

        # 1. 连板高度评分
        continuous_days = stock.get('continuous_days', 0)
        continuous_score = min(continuous_days * 25, 100)  # 每连板一天25分，最高100
        score += continuous_score * self.weights['连板高度']

        # 2. 涨停时间评分（假设有涨停时间数据）
        # 这里我们使用相对评分，假设早涨停的更强
        # 实际应该从数据中获取涨停时间
        limit_time_score = 60  # 默认值
        score += limit_time_score * self.weights['涨停时间']

        # 3. 封单金额评分
        amount = stock.get('amount', 0)
        if amount > 1e9:  # 10亿以上
            amount_score = 100
        elif amount > 5e8:  # 5亿以上
            amount_score = 80
        elif amount > 2e8:  # 2亿以上
            amount_score = 65
        elif amount > 5e7:  # 5千万以上
            amount_score = 50
        else:
            amount_score = 30
        score += amount_score * self.weights['封单金额']

        # 4. 技术形态评分
        features = stock.get('features', {})
        technical_score = 50  # 基础分

        if features.get('is_breakout', False):
            technical_score += 20

        if features.get('price_position', 50) > 70:  # 高位突破
            technical_score += 15
        elif features.get('price_position', 50) < 30:  # 低位启动
            technical_score += 10

        if features.get('trend_strength', 0) > 5:  # 趋势强
            technical_score += 10

        if features.get('volume_ratio', 1) > 2:  # 放量明显
            technical_score += 5

        technical_score = min(technical_score, 100)
        # 将技术形态评分并入流通市值权重中
        score += technical_score * self.weights['流通市值']

        return round(score, 2)

    def _empty_roles(self) -> Dict[str, List]:
        """返回空的角色识别结果"""
        return {
            '龙头': [],
            '中军': [],
            '补涨': [],
            '观察': []
        }