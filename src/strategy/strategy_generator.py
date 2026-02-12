"""
策略生成器
基于分析结果生成交易策略（集成 DeepSeek AI 分析）
"""
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import logging

class StrategyGenerator:
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def generate_strategy(self,
                         analysis_results: Dict,
                         roles_identification: Dict,
                         ai_analysis: Optional[Dict] = None) -> Dict:
        """
        生成交易策略
        :param analysis_results: 涨停分析结果
        :param roles_identification: 角色识别结果（龙头/中军/补涨）
        :param ai_analysis: DeepSeek AI 分析结果（可选）
        """
        strategy = {
            '生成时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '市场概况': {},
            '主线分析': [],
            '个股策略': [],
            '风险提示': [],
            '操作建议': []
        }

        # 市场概况
        strategy['市场概况'] = self._generate_market_summary(analysis_results)

        # 主线分析（可结合 AI 板块判断）
        strategy['主线分析'] = self._identify_main_themes(roles_identification, ai_analysis)

        # 个股策略（深度融合 AI 分析）
        strategy['个股策略'] = self._generate_stock_strategies(roles_identification, ai_analysis)

        # 风险提示
        strategy['风险提示'] = self._generate_risk_warnings(analysis_results, roles_identification)

        # 操作建议
        strategy['操作建议'] = self._generate_trading_suggestions(strategy)

        return strategy

    def _generate_market_summary(self, analysis_results: Dict) -> Dict:
        """生成市场概况（不变）"""
        summary = analysis_results.get('summary', {})
        return {
            '涨停家数': summary.get('total_count', 0),
            '连板高度': self._get_max_continuous_days(analysis_results),
            '封板成功率': self._calculate_success_rate(analysis_results),
            '市场情绪': self._assess_market_sentiment(summary),
            '赚钱效应': self._assess_profit_effect(summary)
        }

    def _identify_main_themes(self, roles: Dict, ai_analysis: Optional[Dict] = None) -> List[Dict]:
        """识别市场主线（增强：加入 AI 板块评级）"""
        themes = []
        sector_stats = {}

        # 统计各板块涨停数据
        for role_type, stocks in roles.items():
            for stock in stocks:
                sector = stock.get('所属板块', '其他')
                if sector not in sector_stats:
                    sector_stats[sector] = {'count': 0, 'roles': [], 'stock_codes': []}
                sector_stats[sector]['count'] += 1
                sector_stats[sector]['roles'].append(role_type)
                sector_stats[sector]['stock_codes'].append(stock.get('code'))

        # 构建板块与 AI 分析的映射（如果提供）
        ai_sector_map = {}
        if ai_analysis:
            # 遍历 AI 分析中的每只股票，提取其板块分析结论
            for role, stocks in ai_analysis.items():
                if role not in ['龙头', '中军', '补涨']:
                    continue
                for item in stocks:
                    stock_info = item.get('stock_info', {})
                    code = stock_info.get('code')
                    # 从 AI 详细分析中尝试提取板块评价关键词
                    detailed = item.get('详细分析', '')
                    # 简单启发：如果分析中包含“板块”字样，记录该板块被 AI 提及
                    if '板块' in detailed:
                        # 实际项目中可更精细地解析，这里简化处理
                        for sector in sector_stats.keys():
                            if sector in detailed:
                                if sector not in ai_sector_map:
                                    ai_sector_map[sector] = []
                                ai_sector_map[sector].append(detailed[:50])  # 保留片段

        # 生成主线板块列表
        for sector, stats in sector_stats.items():
            if stats['count'] >= self.config['analysis']['板块强度阈值']:
                theme = {
                    '板块名称': sector,
                    '涨停家数': stats['count'],
                    '龙头数量': stats['roles'].count('龙头'),
                    '强度评级': self._rate_sector_strength(stats),
                    '持续性判断': self._judge_sector_persistence(stats),
                }
                # 如果 AI 有该板块的分析，追加字段
                if sector in ai_sector_map:
                    theme['AI强度分析'] = '；'.join(ai_sector_map[sector][:2])  # 取前两条
                themes.append(theme)

        themes.sort(key=lambda x: x['涨停家数'], reverse=True)
        return themes[:3]  # 返回最强的3个板块

    def _generate_stock_strategies(self, roles: Dict, ai_analysis: Optional[Dict] = None) -> List[Dict]:
        """
        生成个股策略（深度融合 AI 分析结果）
        """
        strategies = []

        # 建立股票代码 -> AI 分析结果的映射
        ai_map = {}
        if ai_analysis:
            for role in ['龙头', '中军', '补涨']:
                for item in ai_analysis.get(role, []):
                    stock_info = item.get('stock_info', {})
                    code = stock_info.get('code')
                    if code:
                        ai_map[code] = item

        # 处理龙头股
        for dragon in roles.get('龙头', []):
            code = dragon['code']
            ai_item = ai_map.get(code, {})
            strategy_item = {
                '代码': code,
                '名称': dragon['name'],
                '角色': '龙头',
                '策略类型': '核心持仓',
                '操作建议': self._get_dragon_strategy(dragon),
                '买入条件': '分歧低吸或弱转强时',
                '止损位': f"{dragon.get('close', 0) * 0.93:.2f}",
                '目标位': f"{dragon.get('close', 0) * 1.15:.2f}"
            }
            # 添加 AI 分析信息
            self._add_ai_info_to_strategy(strategy_item, ai_item)
            strategies.append(strategy_item)

        # 处理中军股
        for middle in roles.get('中军', []):
            code = middle['code']
            ai_item = ai_map.get(code, {})
            strategy_item = {
                '代码': code,
                '名称': middle['name'],
                '角色': '中军',
                '策略类型': '趋势跟随',
                '操作建议': '5日线附近低吸，趋势持有',
                '买入条件': '回踩5日线不破时',
                '止损位': f"{middle.get('close', 0) * 0.95:.2f}",
                '目标位': f"{middle.get('close', 0) * 1.10:.2f}"
            }
            self._add_ai_info_to_strategy(strategy_item, ai_item)
            strategies.append(strategy_item)

        # 处理补涨股
        for follower in roles.get('补涨', []):
            code = follower['code']
            ai_item = ai_map.get(code, {})
            strategy_item = {
                '代码': code,
                '名称': follower['name'],
                '角色': '补涨',
                '策略类型': '短线套利',
                '操作建议': '竞价强势或首封打板',
                '买入条件': '板块强势时早盘首板',
                '止损位': f"{follower.get('close', 0) * 0.92:.2f}",
                '目标位': f"{follower.get('close', 0) * 1.08:.2f}",
                '备注': '快进快出，注意龙头走势'
            }
            self._add_ai_info_to_strategy(strategy_item, ai_item)
            strategies.append(strategy_item)

        return strategies

    def _add_ai_info_to_strategy(self, strategy_item: Dict, ai_item: Dict):
        """为个股策略添加 AI 分析字段"""
        if not ai_item:
            return

        # 1. 涨停原因 / 消息催化
        limit_up_reasons = ai_item.get('涨停原因', [])
        if limit_up_reasons:
            strategy_item['涨停原因'] = limit_up_reasons
            # 同时生成一个简短的催化摘要（用于快速查看）
            if isinstance(limit_up_reasons, list) and limit_up_reasons:
                strategy_item['消息催化'] = limit_up_reasons[0][:50] + ('...' if len(limit_up_reasons[0]) > 50 else '')

        # 2. AI 分析摘要
        summary = ai_item.get('摘要', '')
        if summary:
            strategy_item['AI分析摘要'] = summary

        # 3. 详细分析（完整版）
        detailed = ai_item.get('详细分析', '')
        if detailed:
            strategy_item['AI详细分析'] = detailed

        # 4. AI 确认的角色（可选）
        if 'stock_info' in ai_item and 'role' in ai_item['stock_info']:
            strategy_item['AI角色'] = ai_item['stock_info']['role']

    # ---------- 以下为原有辅助方法，基本保持不变（仅微调）----------
    def _get_max_continuous_days(self, analysis_results: Dict) -> int:
        max_days = 0
        for pattern in analysis_results.get('patterns', []):
            days = pattern.get('continuous_days', 0)
            max_days = max(max_days, days)
        return max_days

    def _calculate_success_rate(self, analysis_results: Dict) -> str:
        total_count = analysis_results.get('summary', {}).get('total_count', 0)
        if total_count == 0:
            return "0%"
        if total_count > 80:
            return "85%"
        elif total_count > 50:
            return "75%"
        elif total_count > 30:
            return "65%"
        else:
            return "55%"

    def _assess_market_sentiment(self, summary: Dict) -> str:
        count = summary.get('total_count', 0)
        if count > 80:
            return "高潮"
        elif count > 50:
            return "活跃"
        elif count > 30:
            return "温和"
        else:
            return "冰点"

    def _assess_profit_effect(self, summary: Dict) -> str:
        count = summary.get('total_count', 0)
        if count > 60:
            return "好"
        elif count > 40:
            return "一般"
        else:
            return "差"

    def _rate_sector_strength(self, stats: Dict) -> str:
        count = stats['count']
        if count >= 10:
            return "★★★★★"
        elif count >= 7:
            return "★★★★"
        elif count >= 5:
            return "★★★"
        elif count >= 3:
            return "★★"
        else:
            return "★"

    def _judge_sector_persistence(self, stats: Dict) -> str:
        count = stats['count']
        if '龙头' in stats['roles'] and count >= 5:
            return "主线明确，持续性较强"
        elif count >= 3:
            return "有一定持续性，需观察"
        else:
            return "一日游可能性大，谨慎参与"

    def _get_dragon_strategy(self, dragon: Dict) -> str:
        continuous_days = dragon.get('continuous_days', 0)
        if continuous_days >= 5:
            return "持有为主，断板时减仓，反包失败离场"
        elif continuous_days >= 3:
            return "分歧时低吸，加速时持有，放量滞涨时减仓"
        else:
            return "确认龙头地位后加仓，关注板块梯队完整性"

    def _generate_risk_warnings(self, analysis_results: Dict, roles: Dict) -> List[str]:
        """生成风险提示（不变）"""
        warnings = []
        summary = analysis_results.get('summary', {})
        if summary.get('total_count', 0) > 100:
            warnings.append("涨停家数过多，警惕情绪高潮后的分化风险")
        if summary.get('total_count', 0) < 30:
            warnings.append("涨停家数较少，市场情绪低迷，注意仓位控制")
        max_days = self._get_max_continuous_days(analysis_results)
        if max_days >= 7:
            warnings.append(f"最高连板{max_days}天，注意高位股补跌风险")
        if len(roles.get('龙头', [])) == 0:
            warnings.append("无明显龙头板块，市场主线不清晰，谨慎操作")
        return warnings

    def _generate_trading_suggestions(self, strategy: Dict) -> List[str]:
        """生成操作建议（不变）"""
        suggestions = []
        market_status = strategy['市场概况']
        if market_status['市场情绪'] == '高潮':
            suggestions.append("控制仓位，优先处理持仓，谨慎开新仓")
            suggestions.append("关注低位首板或新题材机会")
        elif market_status['市场情绪'] == '冰点':
            suggestions.append("小仓位试错，关注率先走强的板块")
            suggestions.append("重点观察连板股能否打开空间")
        else:
            suggestions.append("去弱留强，聚焦主线板块核心个股")
            suggestions.append("龙头分歧时低吸，跟风股冲高减仓")

        themes = strategy['主线分析']
        if themes:
            main_theme = themes[0]
            suggestions.append(f"重点关注{main_theme['板块名称']}板块，{main_theme['持续性判断']}")
        return suggestions