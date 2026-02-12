"""
涨停板分析核心模块
分析涨停强度、封单、连板等情况
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import logging

class LimitUpAnalyzer:
    def __init__(self, config: Dict):
        self.config = config
        self.limit_threshold = config['analysis']['涨停阈值']
        self.logger = logging.getLogger(__name__)

    def analyze_limit_up(self, limit_up_df: pd.DataFrame,
                        historical_data: Dict[str, pd.DataFrame]) -> Dict:
        """分析涨停板数据"""
        if limit_up_df.empty:
            return self._empty_analysis_result()

        analysis_results = {
            'summary': {},
            'stocks': [],
            'patterns': []
        }

        # 基础统计
        analysis_results['summary'] = {
            'total_count': len(limit_up_df),
            'amount_total': limit_up_df['amount'].sum() / 1e8,  # 转换为亿元
            'volume_total': limit_up_df['volume'].sum(),
            'avg_pct_change': limit_up_df['pct_change'].mean(),
            'avg_amount': limit_up_df['amount'].mean() / 1e8,
            'max_pct_change': limit_up_df['pct_change'].max(),
            'min_pct_change': limit_up_df['pct_change'].min()
        }

        # 分析每只涨停股
        self.logger.info(f"📊 分析 {len(limit_up_df)} 只涨停股票...")
        for idx, row in limit_up_df.iterrows():
            stock_analysis = self._analyze_single_stock(row, historical_data)
            analysis_results['stocks'].append(stock_analysis)

            # 显示进度
            if (idx + 1) % 10 == 0 or (idx + 1) == len(limit_up_df):
                self.logger.info(f"  进度: {idx + 1}/{len(limit_up_df)}")

        # 识别连板股
        self.logger.info("🔍 识别连板股票...")
        continuous_stocks = self._find_continuous_limit_up(analysis_results['stocks'], historical_data)
        analysis_results['patterns'].extend(continuous_stocks)

        # 计算市场情绪
        analysis_results['summary']['market_sentiment'] = self._calculate_market_sentiment(
            analysis_results['summary']['total_count']
        )

        # 计算封板成功率（简化估算）
        analysis_results['summary']['success_rate'] = self._estimate_success_rate(
            analysis_results['summary']['total_count']
        )

        self.logger.info(f"✅ 涨停分析完成，发现 {len(continuous_stocks)} 只连板股票")
        return analysis_results

    def _analyze_single_stock(self, stock_row: pd.Series,
                             historical_data: Dict[str, pd.DataFrame]) -> Dict:
        """分析单只涨停股票"""
        code = stock_row['code']
        analysis = {
            'code': code,
            'name': stock_row.get('name', code),
            'close': float(stock_row['close']) if pd.notna(stock_row['close']) else 0,
            'pct_change': float(stock_row['pct_change']) if pd.notna(stock_row['pct_change']) else 0,
            'amount': float(stock_row['amount']) if pd.notna(stock_row['amount']) else 0,
            'volume': float(stock_row['volume']) if pd.notna(stock_row['volume']) else 0,
            'features': {},
            'technical_indicators': {}
        }

        # 获取历史数据
        if code in historical_data:
            hist_df = historical_data[code]
            if len(hist_df) >= 5:  # 至少有5天数据
                # 计算技术指标
                analysis['technical_indicators'] = self._calculate_technical_indicators(hist_df)

                # 计算特征
                analysis['features']['price_position'] = self._calculate_price_position(hist_df)
                analysis['features']['volume_ratio'] = self._calculate_volume_ratio(hist_df)
                analysis['features']['is_breakout'] = self._check_breakout(hist_df)
                analysis['features']['trend_strength'] = self._calculate_trend_strength(hist_df)

                # 检查连板情况
                analysis['continuous_days'] = self._check_limit_up_days(hist_df)
                analysis['total_increase'] = self._calculate_total_increase(hist_df, analysis['continuous_days'])

        return analysis

    def _find_continuous_limit_up(self, stocks_analysis: List[Dict],
                                 historical_data: Dict[str, pd.DataFrame]) -> List[Dict]:
        """识别连板股票"""
        continuous_stocks = []

        for stock in stocks_analysis:
            code = stock['code']
            if code in historical_data:
                hist_df = historical_data[code]
                # 检查连板天数
                continuous_days = stock.get('continuous_days', 0)

                if continuous_days >= 2:  # 至少2连板
                    stock['continuous_days'] = continuous_days
                    stock['total_increase'] = self._calculate_total_increase(hist_df, continuous_days)
                    stock['daily_increases'] = self._get_daily_increases(hist_df, continuous_days)

                    # 计算连板强度
                    stock['continuous_strength'] = self._calculate_continuous_strength(stock, hist_df)
                    continuous_stocks.append(stock)

        # 按连板天数排序
        continuous_stocks.sort(key=lambda x: x.get('continuous_days', 0), reverse=True)
        return continuous_stocks

    def _calculate_technical_indicators(self, df: pd.DataFrame) -> Dict:
        """计算技术指标"""
        indicators = {}

        if len(df) < 5:
            return indicators

        try:
            closes = pd.to_numeric(df['close'].tail(10), errors='coerce').values
            volumes = pd.to_numeric(df['volume'].tail(10), errors='coerce').values

            if len(closes) >= 5:
                # RSI
                indicators['rsi_5'] = self._calculate_rsi(closes, 5)

                # 移动平均线
                indicators['ma_5'] = np.mean(closes[-5:]) if len(closes) >= 5 else closes[-1]
                indicators['ma_10'] = np.mean(closes[-10:]) if len(closes) >= 10 else closes[-1]

                # 价格位置
                if len(closes) >= 20:
                    indicators['price_position'] = self._calculate_price_position_df(df)

                # 量比
                if len(volumes) >= 6:
                    today_volume = volumes[-1]
                    avg_volume = np.mean(volumes[-6:-1])
                    indicators['volume_ratio'] = today_volume / avg_volume if avg_volume > 0 else 1.0

        except Exception as e:
            self.logger.warning(f"计算技术指标失败: {str(e)}")

        return indicators

    def _calculate_rsi(self, prices: np.ndarray, period: int = 5) -> float:
        """计算RSI指标"""
        if len(prices) < period + 1:
            return 50.0

        deltas = np.diff(prices)
        gain = np.where(deltas > 0, deltas, 0)
        loss = np.where(deltas < 0, -deltas, 0)

        avg_gain = np.mean(gain[:period])
        avg_loss = np.mean(loss[:period])

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return round(rsi, 2)

    def _calculate_price_position(self, df: pd.DataFrame) -> float:
        """计算价格位置（相对于近期高低的百分比）"""
        if len(df) < 20:
            return 50.0

        closes = pd.to_numeric(df['close'].tail(20), errors='coerce')
        if len(closes) < 20:
            return 50.0

        current = closes.iloc[-1]
        lowest = closes.min()
        highest = closes.max()

        if highest == lowest:
            return 50.0

        position = (current - lowest) / (highest - lowest) * 100
        return round(position, 2)

    def _calculate_price_position_df(self, df: pd.DataFrame) -> float:
        """计算价格位置（DataFrame版本）"""
        return self._calculate_price_position(df)

    def _calculate_volume_ratio(self, df: pd.DataFrame) -> float:
        """计算量比"""
        if len(df) < 6:
            return 1.0

        volumes = pd.to_numeric(df['volume'].tail(6), errors='coerce')
        if len(volumes) < 6:
            return 1.0

        today_volume = volumes.iloc[-1]
        avg_volume = volumes.iloc[:-1].mean()

        return round(today_volume / avg_volume if avg_volume > 0 else 1.0, 2)

    def _check_breakout(self, df: pd.DataFrame) -> bool:
        """检查是否突破"""
        if len(df) < 10:
            return False

        closes = pd.to_numeric(df['close'].tail(10), errors='coerce')
        if len(closes) < 10:
            return False

        # 检查是否突破近期高点
        current = closes.iloc[-1]
        prev_max = closes.iloc[:-1].max()

        return current > prev_max * 1.03  # 突破3%以上

    def _calculate_trend_strength(self, df: pd.DataFrame) -> float:
        """计算趋势强度"""
        if len(df) < 5:
            return 0.0

        closes = pd.to_numeric(df['close'].tail(5), errors='coerce')
        if len(closes) < 5:
            return 0.0

        # 计算简单趋势（5日涨幅）
        first_price = closes.iloc[0]
        last_price = closes.iloc[-1]

        if first_price > 0:
            trend = (last_price / first_price - 1) * 100
            return round(trend, 2)

        return 0.0

    def _check_limit_up_days(self, df: pd.DataFrame) -> int:
        """检查连板天数"""
        if len(df) < 2:
            return 0

        # 这里需要根据涨跌幅判断涨停
        # 注意：需要确保有pct_change或close/preclose数据

        continuous_days = 0

        # 从最近一天开始往前检查
        for i in range(min(10, len(df) - 1)):
            idx = -(i + 1)

            try:
                # 尝试获取涨跌幅
                if 'pct_change' in df.columns and pd.notna(df.iloc[idx]['pct_change']):
                    pct_change = float(df.iloc[idx]['pct_change'])
                elif 'close' in df.columns and 'preclose' in df.columns:
                    close_price = float(df.iloc[idx]['close'])
                    preclose_price = float(df.iloc[idx]['preclose'])
                    pct_change = (close_price / preclose_price - 1) * 100 if preclose_price > 0 else 0
                else:
                    break

                if pct_change >= self.limit_threshold:
                    continuous_days += 1
                else:
                    break

            except:
                break

        return continuous_days

    def _calculate_total_increase(self, df: pd.DataFrame, days: int) -> float:
        """计算累计涨幅"""
        if len(df) < days + 1 or days == 0:
            return 0.0

        try:
            start_idx = -(days + 1)
            end_idx = -1

            start_price = float(df.iloc[start_idx]['close'])
            end_price = float(df.iloc[end_idx]['close'])

            if start_price > 0:
                total_increase = (end_price / start_price - 1) * 100
                return round(total_increase, 2)

        except:
            pass

        return 0.0

    def _get_daily_increases(self, df: pd.DataFrame, days: int) -> List[float]:
        """获取每日涨幅"""
        increases = []

        if len(df) < days + 1 or days == 0:
            return increases

        try:
            for i in range(days):
                idx = -(i + 1)
                prev_idx = -(i + 2)

                current_close = float(df.iloc[idx]['close'])
                prev_close = float(df.iloc[prev_idx]['close'])

                if prev_close > 0:
                    daily_increase = (current_close / prev_close - 1) * 100
                    increases.append(round(daily_increase, 2))
                else:
                    increases.append(0.0)

        except:
            pass

        return increases[::-1]  # 反转，使时间顺序正确

    def _calculate_continuous_strength(self, stock: Dict, hist_df: pd.DataFrame) -> float:
        """计算连板强度"""
        strength = 0.0

        # 基于连板天数
        continuous_days = stock.get('continuous_days', 0)
        strength += min(continuous_days * 20, 100) * 0.4

        # 基于成交量（缩量连板更强）
        if len(hist_df) >= 3:
            volumes = pd.to_numeric(hist_df['volume'].tail(3), errors='coerce')
            if len(volumes) >= 3 and volumes.mean() > 0:
                volume_ratio = volumes.iloc[-1] / volumes.mean()
                if volume_ratio < 0.8:  # 缩量
                    strength += 30
                elif volume_ratio < 1.2:  # 平量
                    strength += 20
                else:  # 放量
                    strength += 10

        # 基于封单金额（如果有数据）
        amount = stock.get('amount', 0)
        if amount > 1e9:  # 10亿以上
            strength += 30
        elif amount > 5e8:  # 5亿以上
            strength += 20
        elif amount > 1e8:  # 1亿以上
            strength += 10

        return round(min(strength, 100), 1)

    def _calculate_market_sentiment(self, limit_up_count: int) -> str:
        """计算市场情绪"""
        if limit_up_count > 100:
            return "高潮"
        elif limit_up_count > 60:
            return "活跃"
        elif limit_up_count > 30:
            return "温和"
        elif limit_up_count > 10:
            return "清淡"
        else:
            return "冰点"

    def _estimate_success_rate(self, limit_up_count: int) -> str:
        """估算封板成功率"""
        if limit_up_count == 0:
            return "0%"
        elif limit_up_count > 80:
            return "85%"
        elif limit_up_count > 50:
            return "75%"
        elif limit_up_count > 30:
            return "65%"
        else:
            return "55%"

    def _empty_analysis_result(self) -> Dict:
        """返回空的分析结果"""
        return {
            'summary': {
                'total_count': 0,
                'amount_total': 0,
                'volume_total': 0,
                'avg_pct_change': 0,
                'avg_amount': 0,
                'max_pct_change': 0,
                'min_pct_change': 0,
                'market_sentiment': '冰点',
                'success_rate': '0%'
            },
            'stocks': [],
            'patterns': []
        }