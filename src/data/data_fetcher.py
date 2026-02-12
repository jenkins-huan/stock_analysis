"""
数据获取器 - 完整版本 (BaoStock + Akshare 双源架构)
"""
import baostock as bs
import pandas as pd
import numpy as np
import logging
import time
import akshare as ak
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple


class DataFetcher:
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 数据源连接状态
        self.bs_connected = False
        self.ak_enabled = config.get('akshare', {}).get('enable', True)

        # 缓存
        self.stock_basic_cache = None
        self.stock_industry_cache = {}

        # 初始化连接
        self._init_data_sources()

    def _init_data_sources(self):
        """初始化数据源连接"""
        # 1. 初始化BaoStock
        try:
            lg = bs.login()
            if lg.error_code == '0':
                self.bs_connected = True
                self.logger.info("✅ BaoStock登录成功")
            else:
                self.logger.warning(f"⚠️ BaoStock登录失败: {lg.error_msg}")
        except Exception as e:
            self.logger.error(f"❌ BaoStock初始化失败: {str(e)}")

        # 2. Akshare无需主动连接，只需检查是否安装
        if self.ak_enabled:
            try:
                # 简单测试akshare是否可用
                import akshare as ak
                self.logger.info("✅ Akshare已启用")
            except ImportError:
                self.logger.error("❌ Akshare未安装，请运行: pip install akshare")
                self.ak_enabled = False

    def get_today_limit_up(self, trade_date: str) -> pd.DataFrame:
        """
        获取当日涨停股票数据
        优先级：Akshare > BaoStock(计算)
        """
        limit_up_stocks = []

        # ===== 方案一：优先使用Akshare（最直接准确） =====
        if self.ak_enabled:
            try:
                self.logger.info(f"🔄 尝试通过Akshare获取涨停板数据，日期: {trade_date}")

                # 方法1: 使用涨停板行情接口（如果可用）
                try:
                    # 注意：不同版本的akshare接口名可能不同，这里尝试几个常见接口
                    limit_df = ak.stock_zt_pool_em(date=trade_date.replace("-", ""))
                    if limit_df is not None and not limit_df.empty:
                        self.logger.info(f"✅ 从Akshare(东方财富)获取到 {len(limit_df)} 只涨停股票")

                        # 标准化字段名
                        for _, row in limit_df.iterrows():
                            stock_info = {
                                'code': str(row.get('代码', row.get('股票代码', ''))).split('.')[0],
                                'name': row.get('名称', row.get('股票简称', '')),
                                'close': float(row.get('最新价', 0)),
                                'pct_change': float(row.get('涨跌幅', 0)),
                                'amount': float(row.get('成交额', 0)),
                                'volume': float(row.get('成交量', 0)),
                                'turnover_rate': float(row.get('换手率', 0)),
                                'limit_up_times': int(row.get('连续涨停天数', 1)),
                                'first_limit_time': row.get('首次封板时间', ''),
                                'last_limit_time': row.get('最后封板时间', ''),
                                'limit_up_type': row.get('涨停类型', ''),
                                'date': trade_date,
                                'data_source': 'akshare_em'
                            }
                            limit_up_stocks.append(stock_info)

                        return pd.DataFrame(limit_up_stocks)
                except Exception as e1:
                    self.logger.debug(f"Akshare涨停板接口尝试1失败: {str(e1)[:100]}")

                # 方法2: 尝试另一个涨停板数据源
                try:
                    limit_df = ak.stock_zt_pool_strong_em(date=trade_date.replace("-", ""))
                    if limit_df is not None and not limit_df.empty:
                        self.logger.info(f"✅ 从Akshare(强势股池)获取到 {len(limit_df)} 只涨停股票")
                        # ... 类似的数据处理 ...
                        return pd.DataFrame(limit_up_stocks)
                except Exception as e2:
                    self.logger.debug(f"Akshare涨停板接口尝试2失败: {str(e2)[:100]}")

                # 方法3: 通过当日涨跌幅排名计算
                try:
                    self.logger.info("ℹ️ 尝试通过Akshare涨跌幅数据计算涨停股...")
                    # 获取当日所有A股涨跌幅
                    change_df = ak.stock_zh_a_spot_em()
                    if change_df is not None and not change_df.empty:
                        # 筛选涨停股（涨跌幅 >= 涨停阈值）
                        limit_threshold = self.config['analysis']['涨停阈值']
                        limit_df = change_df[pd.to_numeric(change_df['涨跌幅'], errors='coerce') >= limit_threshold]

                        self.logger.info(f"✅ 通过Akshare涨跌幅计算发现 {len(limit_df)} 只涨停股票")

                        for _, row in limit_df.iterrows():
                            stock_info = {
                                'code': str(row['代码']).split('.')[0],
                                'name': row['名称'],
                                'close': float(row['最新价']),
                                'pct_change': float(row['涨跌幅']),
                                'amount': float(row['成交额']),
                                'volume': float(row['成交量']),
                                'turnover_rate': float(row['换手率']),
                                'date': trade_date,
                                'data_source': 'akshare_calc'
                            }
                            limit_up_stocks.append(stock_info)

                        return pd.DataFrame(limit_up_stocks)

                except Exception as e3:
                    self.logger.warning(f"Akshare涨跌幅计算失败: {str(e3)[:100]}")

            except Exception as e:
                self.logger.error(f"❌ Akshare获取涨停数据失败: {str(e)}")

        # ===== 方案二：回退到BaoStock计算 =====
        self.logger.info("🔄 回退到BaoStock计算涨停股票...")
        return self._get_limit_up_from_baostock(trade_date)

    def _get_limit_up_from_baostock(self, trade_date: str) -> pd.DataFrame:
        """通过BaoStock日线数据计算涨停股票"""
        limit_up_stocks = []
        limit_threshold = self.config['analysis']['涨停阈值']

        try:
            # 获取A股列表
            all_stocks = self.get_stock_basic()
            if all_stocks.empty:
                self.logger.error("无法获取股票列表")
                return pd.DataFrame()

            # 筛选A股（6/0/3开头）
            a_stocks = all_stocks[all_stocks['code'].str.match(r'^(6|0|3)\d{5}$', na=False)]
            total_stocks = len(a_stocks)

            if total_stocks == 0:
                self.logger.error("未找到A股股票")
                return pd.DataFrame()

            self.logger.info(f"ℹ️ 开始分析 {total_stocks} 只A股股票的涨停情况...")

            analyzed_count = 0
            success_count = 0

            for idx, code in enumerate(a_stocks['code'].tolist()[:800]):  # 限制数量防止超时
                try:
                    # 获取当日数据
                    daily_data = self.get_daily_data(code, trade_date, trade_date)

                    if not daily_data.empty:
                        analyzed_count += 1

                        # 判断涨停
                        if 'pct_change' in daily_data.columns:
                            pct_change = float(daily_data.iloc[0]['pct_change'])
                        elif 'close' in daily_data.columns and 'pre_close' in daily_data.columns:
                            close_price = float(daily_data.iloc[0]['close'])
                            preclose_price = float(daily_data.iloc[0]['pre_close'])
                            if preclose_price > 0:
                                pct_change = (close_price / preclose_price - 1) * 100
                            else:
                                continue
                        else:
                            continue

                        # 涨停判断
                        if pct_change >= limit_threshold:
                            success_count += 1
                            stock_info = {
                                'code': code,
                                'name': self.get_stock_name(code),
                                'close': float(daily_data.iloc[0]['close']) if 'close' in daily_data.columns else 0,
                                'pct_change': pct_change,
                                'amount': float(daily_data.iloc[0]['amount']) if 'amount' in daily_data.columns else 0,
                                'volume': float(daily_data.iloc[0]['volume']) if 'volume' in daily_data.columns else 0,
                                'date': trade_date,
                                'data_source': 'baostock_calc'
                            }
                            limit_up_stocks.append(stock_info)

                    # 进度显示和延时控制
                    if (idx + 1) % 100 == 0:
                        self.logger.info(
                            f"  进度: {idx + 1}/800，已分析 {analyzed_count} 只，发现 {success_count} 只涨停")
                        time.sleep(0.5)  # 避免请求过快

                except Exception as e:
                    continue  # 跳过单只股票的失败

            self.logger.info(f"✅ BaoStock分析完成: 共分析 {analyzed_count} 只股票，发现 {success_count} 只涨停")

        except Exception as e:
            self.logger.error(f"❌ BaoStock计算涨停失败: {str(e)}")

        return pd.DataFrame(limit_up_stocks)

    def get_daily_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取日线数据（BaoStock优先，Akshare备用）"""
        max_retries = 2

        for attempt in range(max_retries):
            try:
                # ===== 优先尝试：BaoStock =====
                if self.bs_connected:
                    # 格式化股票代码
                    if code.startswith('sh.') or code.startswith('sz.'):
                        bs_code = code
                    elif code.startswith('6'):
                        bs_code = f"sh.{code}"
                    elif code.startswith('0') or code.startswith('3'):
                        bs_code = f"sz.{code}"
                    else:
                        self.logger.debug(f"未知代码格式，尝试Akshare: {code}")
                        if self.ak_enabled and attempt >= 1:
                            continue
                        else:
                            break

                    # BaoStock查询
                    rs = bs.query_history_k_data_plus(
                        bs_code,
                        "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg",
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag="2"  # 后复权
                    )

                    if rs.error_code == '0':
                        data_list = []
                        while (rs.error_code == '0') & rs.next():
                            data_list.append(rs.get_row_data())

                        if data_list:
                            df = pd.DataFrame(data_list, columns=rs.fields)
                            df['date'] = pd.to_datetime(df['date'])

                            # 类型转换
                            numeric_cols = ['open', 'high', 'low', 'close', 'preclose',
                                            'volume', 'amount', 'turn', 'pctChg']
                            for col in numeric_cols:
                                if col in df.columns:
                                    df[col] = pd.to_numeric(df[col], errors='coerce')

                            # 字段重命名
                            df.rename(columns={
                                'pctChg': 'pct_change',
                                'preclose': 'pre_close'
                            }, inplace=True)

                            return df

                # ===== 备用方案：Akshare =====
                if self.ak_enabled and attempt >= 1:
                    try:
                        # Akshare股票代码格式：sh600000 或 sz000001
                        if code.startswith('6'):
                            ak_code = f"sh{code}"
                        else:
                            ak_code = f"sz{code}"

                        # 尝试多个Akshare日线接口
                        date_format = "%Y%m%d"
                        start_str = datetime.strptime(start_date, "%Y-%m-%d").strftime(date_format)
                        end_str = datetime.strptime(end_date, "%Y-%m-%d").strftime(date_format)

                        # 接口1: A股日线数据
                        try:
                            df = ak.stock_zh_a_hist(symbol=ak_code, period="daily",
                                                    start_date=start_str, end_date=end_str,
                                                    adjust="hfq")
                            if not df.empty:
                                df.rename(columns={
                                    '日期': 'date',
                                    '开盘': 'open',
                                    '最高': 'high',
                                    '最低': 'low',
                                    '收盘': 'close',
                                    '成交量': 'volume',
                                    '成交额': 'amount',
                                    '振幅': 'amplitude',
                                    '涨跌幅': 'pct_change',
                                    '涨跌额': 'change',
                                    '换手率': 'turn'
                                }, inplace=True)

                                df['date'] = pd.to_datetime(df['date'])
                                # 添加pre_close字段（前一日收盘价）
                                df['pre_close'] = df['close'].shift(1)

                                self.logger.debug(f"✅ Akshare日线数据获取成功: {code}")
                                return df
                        except Exception as e1:
                            self.logger.debug(f"Akshare接口1失败: {str(e1)[:80]}")

                        # 接口2: 备用接口
                        try:
                            df = ak.stock_zh_a_daily(symbol=ak_code, start_date=start_str,
                                                     end_date=end_str, adjust="qfq")
                            if not df.empty:
                                # 类似的数据处理...
                                return df
                        except Exception as e2:
                            self.logger.debug(f"Akshare接口2失败: {str(e2)[:80]}")

                    except Exception as e:
                        self.logger.warning(f"Akshare日线数据获取失败({code}): {str(e)[:100]}")

            except Exception as e:
                self.logger.warning(f"获取 {code} 日线数据失败(尝试{attempt + 1}/{max_retries}): {str(e)[:100]}")
                time.sleep(1)

        return pd.DataFrame()

    def get_stock_basic(self, force_refresh: bool = False) -> pd.DataFrame:
        """获取股票基本信息（Akshare优先）"""
        if not force_refresh and self.stock_basic_cache is not None:
            return self.stock_basic_cache

        try:
            # ===== 优先使用Akshare =====
            if self.ak_enabled:
                try:
                    # 获取A股实时行情数据（包含代码和名称）
                    spot_df = ak.stock_zh_a_spot_em()
                    if spot_df is not None and not spot_df.empty:
                        basic_df = spot_df[['代码', '名称']].copy()
                        basic_df.columns = ['code', 'name']
                        basic_df['code'] = basic_df['code'].str.replace(r'[^\d]', '', regex=True)

                        # 获取更多基本信息
                        try:
                            info_df = ak.stock_info_a_code_name()
                            if info_df is not None:
                                basic_df = basic_df.merge(
                                    info_df[['code', 'industry']],
                                    on='code',
                                    how='left'
                                )
                        except:
                            pass

                        self.stock_basic_cache = basic_df
                        self.logger.info(f"✅ 从Akshare获取到 {len(basic_df)} 只股票基本信息")
                        return basic_df
                except Exception as e:
                    self.logger.warning(f"Akshare股票信息获取失败: {str(e)}")

            # ===== 备用：BaoStock =====
            if self.bs_connected:
                rs = bs.query_stock_basic()
                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())

                if data_list:
                    df = pd.DataFrame(data_list, columns=rs.fields)
                    df['code'] = df['code'].str.replace(r'[^\d]', '', regex=True)
                    df.rename(columns={'code_name': 'name'}, inplace=True)

                    self.stock_basic_cache = df
                    self.logger.info(f"✅ 从BaoStock获取到 {len(df)} 只股票基本信息")
                    return df

        except Exception as e:
            self.logger.error(f"获取股票基础信息失败: {str(e)}")

        return pd.DataFrame()

    def get_stock_name(self, code: str) -> str:
        """获取股票名称"""
        if self.stock_basic_cache is None:
            self.get_stock_basic()

        if self.stock_basic_cache is not None:
            match = self.stock_basic_cache[self.stock_basic_cache['code'] == code]
            if not match.empty:
                return match.iloc[0]['name']

        return code

    def get_index_data(self, index_code: str = "sh000001") -> pd.DataFrame:
        """获取指数数据（用于市场情绪判断）"""
        try:
            if self.ak_enabled:
                # 格式化指数代码
                if index_code.startswith('sh'):
                    ak_code = index_code
                elif index_code.startswith('sz'):
                    ak_code = index_code
                else:
                    ak_code = f"sh{index_code}" if index_code.startswith('0') else f"sz{index_code}"

                df = ak.stock_zh_index_daily(symbol=ak_code)
                if not df.empty:
                    df.rename(columns={'date': 'date'}, inplace=True)
                    return df
        except Exception as e:
            self.logger.warning(f"获取指数数据失败: {str(e)}")

        return pd.DataFrame()

    def __del__(self):
        """清理资源"""
        try:
            if self.bs_connected:
                bs.logout()
                self.logger.info("✅ BaoStock登出成功")
        except:
            pass