# A股打板复盘系统 - 完整主程序
# 修复异步调用问题

import yaml
import logging
import sys
import os
import json
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# 添加项目根目录到路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

class DeepSeekStockAnalyzer:
    """DeepSeek AI分析器"""
    def __init__(self, api_key: str, base_url: str = "https://api.deepseek.com/v1/chat/completions"):
        self.api_key = api_key
        self.base_url = base_url
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def analyze_stock_group(self, stocks_data: List[Dict]) -> List[Dict]:
        """批量分析股票组"""
        if not self.session:
            self.session = aiohttp.ClientSession()

        tasks = []
        for stock_data in stocks_data:
            task = self._analyze_single_stock(stock_data)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 处理结果，将异常转换为空字典
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                processed_results.append({})
            else:
                processed_results.append(result)

        return processed_results

    async def _analyze_single_stock(self, stock_data: Dict) -> Dict:
        """分析单只股票"""
        try:
            prompt = self._build_stock_analysis_prompt(stock_data)

            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }

            payload = {
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": "你是一名资深A股分析师，擅长分析涨停原因和消息催化。"},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 1000,
                "temperature": 0.3
            }

            async with self.session.post(self.base_url, headers=headers, json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    content = result["choices"][0]["message"]["content"]
                    return self._parse_analysis_result(content, stock_data)
                else:
                    return {}

        except Exception as e:
            return {}

    def _build_stock_analysis_prompt(self, stock_data: Dict) -> str:
        """构建分析提示词"""
        return f"""请分析以下股票涨停的原因和消息催化：

股票信息：
名称：{stock_data.get('name', '未知')}
代码：{stock_data.get('code', '未知')}
角色：{stock_data.get('role', '未知')}
连板天数：{stock_data.get('limit_up_days', 0)}天
流通市值：{stock_data.get('market_cap', 0)}亿元
所属行业：{', '.join(stock_data.get('industry', ['未知']))}
核心概念：{', '.join(stock_data.get('core_sectors', ['未知']))}

近期价格走势：
最新价格：{stock_data.get('recent_price', {}).get('latest_price', 0) if isinstance(stock_data.get('recent_price'), dict) else 0}
涨跌幅：{stock_data.get('recent_price', {}).get('change_percent', 0) if isinstance(stock_data.get('recent_price'), dict) else 0}%
趋势：{stock_data.get('recent_price', {}).get('trend', '未知') if isinstance(stock_data.get('recent_price'), dict) else '未知'}

相关新闻：
{chr(10).join(stock_data.get('recent_news', []))}

请从以下角度进行结构化分析：
1. **直接消息催化**：哪些具体消息、公告、政策导致了涨停？
2. **板块效应**：所属板块整体表现如何？是否是板块龙头？
3. **技术面分析**：资金流向、技术形态、突破情况。
4. **基本面支撑**：业绩、估值、行业地位等。
5. **持续性判断**：涨停势头是否可持续？后续可能走势。
6. **风险提示**：需要关注哪些风险？

要求：分析要具体、有逻辑性，给出明确的判断依据。"""

    def _parse_analysis_result(self, content: str, stock_data: Dict) -> Dict:
        """解析AI分析结果"""
        return {
            "股票信息": f"{stock_data.get('name')}({stock_data.get('code')})",
            "分析时间": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "详细分析": content,
            "摘要": self._extract_summary(content),
            "涨停原因": self._extract_limit_up_reasons(content)
        }

    def _extract_summary(self, content: str) -> str:
        """提取分析摘要"""
        # 简单提取前200字作为摘要
        return content[:200] + "..." if len(content) > 200 else content

    def _extract_limit_up_reasons(self, content: str) -> List[str]:
        """提取涨停原因"""
        reasons = []
        keywords = ["消息催化", "政策", "公告", "业绩", "技术突破", "资金流入", "板块轮动"]

        for line in content.split('\n'):
            if any(keyword in line for keyword in keywords):
                reasons.append(line.strip())

        return reasons if reasons else ["综合分析推动涨停"]

class StockReviewSystem:
    def __init__(self, config_path: str = None):
        """初始化复盘系统"""
        # 确定配置文件路径
        self.config_path = self._find_config_file(config_path)

        # 加载配置
        self.config = self._load_config()

        # 设置日志
        self._setup_logging()
        self.logger = logging.getLogger(__name__)

        # 初始化组件
        self._init_components()

        # 运行统计
        self.run_stats = {
            'start_time': None,
            'end_time': None,
            'success': False,
            'error': None
        }

    def _find_config_file(self, config_path: str) -> str:
        """查找配置文件"""
        if config_path and os.path.exists(config_path):
            return config_path

        # 尝试多个可能的配置文件路径
        possible_paths = [
            "config.yaml",  # 根目录
            os.path.join(project_root, "config.yaml"),
            "C:/Users/Jenkins/Desktop/stock_analysis/config.yaml",
            "../config.yaml"
        ]

        for path in possible_paths:
            if os.path.exists(path):
                print(f"找到配置文件: {path}")
                return path

        # 如果没找到，尝试创建默认配置文件
        print("未找到配置文件，尝试创建默认配置...")
        self._create_default_config()
        return "config.yaml"

    def _create_default_config(self):
        """创建默认配置文件"""
        config_content = """# 数据源配置
data_sources:
  primary: baostock
  backup: tushare
  
# BaoStock配置
baostock:
  username: ""
  password: ""
  
# Tushare配置
tushare:
  token: "your_tushare_token_here"
  timeout: 30
  
# 分析参数
analysis:
  涨停阈值: 9.8
  连板天数: 3
  板块强度阈值: 3
  龙头评分权重:
    连板高度: 0.35
    涨停时间: 0.25
    封单金额: 0.20
    流通市值: 0.20

# DeepSeek AI配置
deepseek:
  api_key: "your_deepseek_api_key_here"
  base_url: "https://api.deepseek.com/v1/chat/completions"
  enable: false
  analyze_roles: ["龙头"]
  max_tokens: 1000
   
# 微信机器人配置
wechat:
  webhook: "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=your_key_here"
  enable: false
  
# 运行配置
schedule:
  run_time: "18:00"
  market_days_only: true
"""

        with open("config.yaml", "w", encoding="utf-8") as f:
            f.write(config_content)
        print("已创建默认配置文件: config.yaml")
        print("请编辑此文件，替换 tushare token 等信息")

    def _load_config(self) -> Dict:
        """加载配置文件"""
        print(f"正在加载配置文件: {self.config_path}")
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config

    def _setup_logging(self):
        """配置日志"""
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_file = os.path.join(log_dir, f"review_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )

        # 同时创建一个简化的运行日志
        self.run_log_file = os.path.join(log_dir, "run_history.log")

    def _init_components(self):
        """初始化所有系统组件"""
        self.components_ready = False

        # 必须初始化的核心组件
        self.data_fetcher = None
        self.limit_up_analyzer = None
        self.sector_analyzer = None
        self.dragon_identifier = None
        self.strategy_generator = None
        self.notifier = None

        component_errors = []

        try:
            # 1. 数据获取器
            try:
                from src.data.data_fetcher import DataFetcher
                self.data_fetcher = DataFetcher(self.config)
                self.logger.info("✅ 数据获取器初始化成功")
            except ImportError as e:
                component_errors.append(f"数据获取器导入失败: {str(e)}")
                self.logger.error(f"❌ 无法导入DataFetcher模块: {str(e)}")
            except Exception as e:
                component_errors.append(f"数据获取器初始化失败: {str(e)}")
                self.logger.error(f"❌ DataFetcher初始化失败: {str(e)}")

            # 2. 涨停分析器
            if self.data_fetcher:
                try:
                    from src.analysis.limit_up_analyzer import LimitUpAnalyzer
                    self.limit_up_analyzer = LimitUpAnalyzer(self.config)
                    self.logger.info("✅ 涨停分析器初始化成功")
                except Exception as e:
                    component_errors.append(f"涨停分析器失败: {str(e)}")
                    self.logger.error(f"❌ 涨停分析器初始化失败: {str(e)}")

                # 3. 板块分析器
                try:
                    from src.analysis.sector_analyzer import SectorAnalyzer
                    self.sector_analyzer = SectorAnalyzer(self.config)
                    self.logger.info("✅ 板块分析器初始化成功")
                except Exception as e:
                    component_errors.append(f"板块分析器失败: {str(e)}")
                    self.logger.error(f"❌ 板块分析器初始化失败: {str(e)}")

                # 4. 龙头识别器
                try:
                    from src.analysis.dragon_head_identifier import DragonHeadIdentifier
                    self.dragon_identifier = DragonHeadIdentifier(self.config)
                    self.logger.info("✅ 龙头识别器初始化成功")
                except Exception as e:
                    component_errors.append(f"龙头识别器失败: {str(e)}")
                    self.logger.error(f"❌ 龙头识别器初始化失败: {str(e)}")

                # 5. 策略生成器
                try:
                    from src.strategy.strategy_generator import StrategyGenerator
                    self.strategy_generator = StrategyGenerator(self.config)
                    self.logger.info("✅ 策略生成器初始化成功")
                except Exception as e:
                    component_errors.append(f"策略生成器失败: {str(e)}")
                    self.logger.error(f"❌ 策略生成器初始化失败: {str(e)}")

                # 6. 微信通知器
                try:
                    from src.notification.wechat_notifier import WechatNotifier
                    self.notifier = WechatNotifier(self.config)
                    if self.config['wechat']['enable']:
                        self.logger.info("✅ 微信通知器初始化成功（已启用）")
                    else:
                        self.logger.info("✅ 微信通知器初始化成功（未启用）")
                except Exception as e:
                    component_errors.append(f"微信通知器失败: {str(e)}")
                    self.logger.error(f"❌ 微信通知器初始化失败: {str(e)}")

                # 检查核心组件是否就绪
                if self.data_fetcher and self.limit_up_analyzer:
                    self.components_ready = True
                    self.logger.info("✅ 所有核心组件初始化完成，系统就绪")
                else:
                    self.logger.error("❌ 核心组件未完全初始化，系统无法运行")

        except Exception as e:
            self.logger.error(f"❌ 组件初始化过程发生未知错误: {str(e)}", exc_info=True)
            component_errors.append(f"初始化过程异常: {str(e)}")

        if component_errors:
            self.logger.warning(f"组件初始化共发现 {len(component_errors)} 个问题:")

    def run(self, trade_date: str = None, test_mode: bool = False):
        """运行完整的复盘流程（同步版本，内部处理异步）"""
        self.run_stats['start_time'] = datetime.now()

        try:
            self.logger.info("=" * 60)
            self.logger.info("A股打板复盘系统 - 开始运行")
            self.logger.info("=" * 60)

            # 检查组件状态
            if not self.components_ready:
                self.logger.error("系统组件初始化失败，无法运行")
                raise RuntimeError("系统组件未就绪")

            # 1. 确定交易日
            if not trade_date:
                trade_date = self._get_trade_date()

            self.logger.info(f"📅 分析日期: {trade_date}")

            # 2. 获取市场数据
            self.logger.info("📊 步骤1: 获取市场数据...")
            market_data = self._fetch_market_data(trade_date)

            if market_data['limit_up_df'].empty:
                self.logger.warning("⚠️ 当日无涨停股票，生成空报告")
                strategy = self._generate_empty_report(trade_date)
            else:
                # 3. 分析涨停板
                self.logger.info("🚀 步骤2: 分析涨停板...")
                limit_up_analysis = self._analyze_limit_up(market_data)

                # 4. 分析板块效应
                self.logger.info("🏢 步骤3: 分析板块效应...")
                sector_analysis = self._analyze_sectors(market_data)

                # 5. 识别龙头/中军/补涨
                self.logger.info("👑 步骤4: 识别龙头/中军/补涨...")
                roles = self._identify_roles(limit_up_analysis, sector_analysis)

                # 6. DeepSeek AI分析（使用asyncio运行异步函数）
                if self.config.get('deepseek', {}).get('enable', False):
                    self.logger.info("🧠 步骤5: DeepSeek AI分析...")
                    try:
                        # 运行异步AI分析函数
                        ai_analysis = asyncio.run(self._analyze_with_deepseek(roles, trade_date))
                        self.logger.info(f"✅ DeepSeek分析完成: {len(ai_analysis.get('龙头', []))}只龙头股分析")
                    except Exception as e:
                        self.logger.error(f"DeepSeek分析失败: {str(e)}")
                        ai_analysis = {}
                else:
                    self.logger.info("⏭️  跳过DeepSeek AI分析（未启用）")
                    ai_analysis = {}
                strategy = self._generate_strategy(limit_up_analysis, roles, trade_date, ai_analysis)
                # 7. 生成策略
                self.logger.info("💡 步骤6: 生成交易策略...")
                strategy = self._generate_strategy(limit_up_analysis, roles, trade_date)

                # 8. 整合AI分析结果
                if ai_analysis:
                    strategy = self._integrate_ai_analysis(strategy, ai_analysis)

            # 9. 保存结果
            self.logger.info("💾 步骤7: 保存结果...")
            self._save_results(strategy, trade_date)

            # 10. 发送通知
            self.logger.info("📤 步骤8: 发送通知...")
            self._send_notifications(strategy, trade_date)

            # 11. 记录运行统计
            self.run_stats['end_time'] = datetime.now()
            self.run_stats['success'] = True
            self._record_run_stats(trade_date)

            self.logger.info("✅ 复盘完成！")
            self.logger.info(f"⏱️  总用时: {(self.run_stats['end_time'] - self.run_stats['start_time']).total_seconds():.1f}秒")

            if test_mode:
                return strategy

        except Exception as e:
            self.run_stats['end_time'] = datetime.now()
            self.run_stats['error'] = str(e)
            self._record_run_stats(trade_date or "unknown")

            self.logger.error(f"❌ 复盘运行失败: {str(e)}", exc_info=True)
            self._send_error_notification(str(e), trade_date)
            raise

    async def _analyze_with_deepseek(self, roles_dict: Dict, trade_date: str) -> Dict:
        """
        异步函数：使用DeepSeek API分析龙头股
        """
        if not self.config.get('deepseek', {}).get('api_key'):
            self.logger.warning("DeepSeek API密钥未配置，跳过AI分析")
            return {}

        self.logger.info("🤖 开始DeepSeek AI梯队分析...")

        all_ai_analysis = {
            'trade_date': trade_date,
            'analyzed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '龙头': [],
            '中军': [],
            '补涨': []
        }

        # 获取需要分析的角色
        analyze_roles = self.config.get('deepseek', {}).get('analyze_roles', ['龙头'])

        async with DeepSeekStockAnalyzer(
            api_key=self.config['deepseek']['api_key'],
            base_url=self.config.get('deepseek', {}).get('base_url', 'https://api.deepseek.com/v1/chat/completions')
        ) as analyzer:

            for role_name in analyze_roles:
                stock_list = roles_dict.get(role_name, [])
                if not stock_list:
                    self.logger.info(f"  ⏭️  {role_name}股: 无股票可分析")
                    continue

                self.logger.info(f"  🔍 分析{role_name}股: {len(stock_list)} 只")

                # 准备股票数据
                stocks_for_analysis = []
                for stock in stock_list:
                    stock_data = await self._prepare_stock_data_for_ai(stock, role_name, trade_date)
                    if stock_data:
                        stocks_for_analysis.append(stock_data)

                if not stocks_for_analysis:
                    continue

                # 批量分析
                try:
                    analysis_results = await analyzer.analyze_stock_group(stocks_for_analysis)

                    # 处理结果
                    valid_results = []
                    for i, result in enumerate(analysis_results):
                        if isinstance(result, dict) and result:
                            result['stock_info'] = {
                                'code': stocks_for_analysis[i]['code'],
                                'name': stocks_for_analysis[i]['name'],
                                'role': role_name
                            }
                            valid_results.append(result)
                        else:
                            self.logger.warning(f"    ⚠️  {stocks_for_analysis[i]['code']} 分析结果无效")

                    all_ai_analysis[role_name] = valid_results
                    self.logger.info(f"    ✅ {role_name}股分析完成: {len(valid_results)}/{len(stock_list)} 只成功")

                except Exception as e:
                    self.logger.error(f"    ❌ {role_name}股分析失败: {str(e)[:100]}")
                    all_ai_analysis[role_name] = []

        # 统计结果
        success_count = sum(len(results) for results in all_ai_analysis.values()
                          if isinstance(results, list))
        self.logger.info(f"📈 DeepSeek分析完成: {success_count} 只股票分析成功")

        return all_ai_analysis

    async def _prepare_stock_data_for_ai(self, stock: Dict, role: str, trade_date: str) -> Dict:
        """异步准备股票数据用于AI分析"""
        # 基础信息
        stock_data = {
            'code': stock.get('code', ''),
            'name': stock.get('name', '未知'),
            'role': role,
            'trade_date': trade_date,
            'limit_up_days': stock.get('limit_up_days', 0),
            'market_cap': stock.get('market_cap', 0)
        }

        try:
            # 获取价格数据
            price_data = self._get_stock_price_data(stock['code'], trade_date)
            if price_data:
                stock_data['recent_price'] = {
                    'latest_price': price_data.get('latest_price', 0),
                    'change_percent': price_data.get('change_percent', 0),
                    'trend': price_data.get('trend', '持平')
                }
        except Exception as e:
            self.logger.debug(f"获取股票{stock['code']}价格数据失败: {str(e)[:50]}")

        try:
            # 获取板块信息
            sectors = self._get_stock_sectors(stock['code'])
            stock_data['industry'] = sectors.get('industry', ['未知行业'])
            stock_data['core_sectors'] = sectors.get('concept', ['未知概念'])
        except Exception as e:
            self.logger.debug(f"获取股票{stock['code']}板块信息失败: {str(e)[:50]}")
            stock_data['industry'] = ['未知行业']
            stock_data['core_sectors'] = ['未知概念']

        try:
            # 获取新闻信息（这里可以调用异步新闻获取）
            news_list = await self._fetch_stock_news_async(stock['code'], trade_date)
            stock_data['recent_news'] = news_list
        except Exception as e:
            self.logger.debug(f"获取股票{stock['code']}新闻失败: {str(e)[:50]}")
            stock_data['recent_news'] = []

        return stock_data

    async def _fetch_stock_news_async(self, stock_code: str, trade_date: str, limit: int = 5) -> List[str]:
        """异步获取股票新闻"""
        try:
            # 这里可以集成真实的新闻API
            # 示例：使用东方财富、新浪财经等数据源

            # 临时返回模拟数据
            return [
                f"{stock_code}所属板块近期有利好政策出台",
                f"市场资金关注{stock_code}所在行业轮动机会",
                f"{stock_code}技术形态突破，受到市场关注",
                f"分析师看好{stock_code}未来发展前景",
                f"{stock_code}近期成交量显著放大"
            ]
        except Exception as e:
            self.logger.warning(f"获取新闻失败: {str(e)}")
            return []

    def _get_stock_price_data(self, code: str, trade_date: str) -> Dict:
        """获取股票价格数据"""
        try:
            if hasattr(self, 'data_fetcher') and self.data_fetcher:
                # 获取最近5个交易日的数据
                end_date = datetime.strptime(trade_date, '%Y-%m-%d')
                start_date = end_date - timedelta(days=10)

                hist_data = self.data_fetcher.get_daily_data(
                    code,
                    start_date.strftime('%Y-%m-%d'),
                    trade_date
                )

                if not hist_data.empty and len(hist_data) > 0:
                    latest = hist_data.iloc[-1]
                    prev = hist_data.iloc[-2] if len(hist_data) > 1 else latest

                    change_percent = ((latest['close'] - prev['close']) / prev['close'] * 100
                                    if prev['close'] > 0 else 0)

                    # 判断趋势
                    if len(hist_data) >= 5:
                        closes = hist_data['close'].tail(5).values
                        if all(closes[i] <= closes[i + 1] for i in range(4)):
                            trend = '上涨'
                        elif all(closes[i] >= closes[i + 1] for i in range(4)):
                            trend = '下跌'
                        else:
                            trend = '震荡'
                    else:
                        trend = '持平' if abs(change_percent) < 2 else ('上涨' if change_percent > 0 else '下跌')

                    return {
                        'latest_price': latest['close'],
                        'change_percent': round(change_percent, 2),
                        'trend': trend,
                        'volume': latest.get('volume', 0)
                    }
        except Exception as e:
            self.logger.debug(f"_get_stock_price_data失败: {str(e)[:50]}")

        return {}

    def _get_stock_sectors(self, code: str) -> Dict:
        """获取股票板块信息"""
        # 这里可以使用你的DataFetcher获取板块信息
        # 示例返回结构
        return {
            'concept': ['国企改革', '上海板块', '纺织服装'],
            'industry': ['纺织业'],
            'region': ['上海']
        }

    def _integrate_ai_analysis(self, strategy: Dict, ai_analysis: Dict) -> Dict:
        """将AI分析结果整合到策略报告中"""
        if not ai_analysis:
            return strategy

        # 创建AI分析部分
        strategy['AI深度分析'] = {
            '分析时间': ai_analysis.get('analyzed_at'),
            '分析日期': ai_analysis.get('trade_date'),
            '梯队分析': {}
        }

        # 按角色整合分析结果
        for role in ['龙头', '中军', '补涨']:
            role_analysis = ai_analysis.get(role, [])
            if role_analysis:
                strategy['AI深度分析']['梯队分析'][role] = []

                for stock_analysis in role_analysis:
                    stock_info = stock_analysis.get('stock_info', {})

                    # 结构化每只股票的AI分析
                    formatted_analysis = {
                        '股票': f"{stock_info.get('name', '')}({stock_info.get('code', '')})",
                        '涨停原因分析': stock_analysis.get('详细分析', '暂无分析'),
                        '消息催化': stock_analysis.get('涨停原因', ['综合分析']),
                        '分析摘要': stock_analysis.get('摘要', '暂无摘要')
                    }

                    strategy['AI深度分析']['梯队分析'][role].append(formatted_analysis)

        # 添加AI分析摘要
        strategy['AI深度分析']['分析摘要'] = self._generate_ai_summary(ai_analysis)

        return strategy

    def _generate_ai_summary(self, ai_analysis: Dict) -> str:
        """生成AI分析摘要"""
        summary_parts = []

        for role in ['龙头', '中军', '补涨']:
            stocks = ai_analysis.get(role, [])
            if stocks:
                summary_parts.append(f"{role}股{len(stocks)}只")

        if summary_parts:
            return f"DeepSeek AI分析了{', '.join(summary_parts)}，提供了涨停原因、消息催化、持续性判断等深度分析。"

        return "AI分析结果为空。"

    def _get_trade_date(self) -> str:
        """获取交易日"""
        today = datetime.now()

        if today.weekday() >= 5:
            days_to_friday = (today.weekday() - 4) % 7
            trade_date = today - timedelta(days=days_to_friday)
        else:
            trade_date = today

        if today.hour < 15:
            trade_date = trade_date - timedelta(days=1)
            while trade_date.weekday() >= 5:
                trade_date = trade_date - timedelta(days=1)

        return trade_date.strftime('%Y-%m-%d')

    def _fetch_market_data(self, trade_date: str) -> Dict[str, Any]:
        """获取市场数据"""
        data = {
            'limit_up_df': pd.DataFrame(),
            'historical_data': {},
            'trade_date': trade_date
        }

        self.logger.info(f"正在获取{trade_date}的涨停股票数据...")
        limit_up_df = self.data_fetcher.get_today_limit_up(trade_date)

        if limit_up_df.empty:
            self.logger.warning(f"{trade_date} 无涨停股票")
            return data

        data['limit_up_df'] = limit_up_df
        self.logger.info(f"获取到 {len(limit_up_df)} 只涨停股票")

        start_date = (datetime.strptime(trade_date, '%Y-%m-%d') -
                     timedelta(days=30)).strftime('%Y-%m-%d')

        self.logger.info(f"正在获取涨停股票的历史数据...")

        success_count = 0
        for idx, row in limit_up_df.iterrows():
            code = row['code']
            try:
                hist_data = self.data_fetcher.get_daily_data(code, start_date, trade_date)
                if not hist_data.empty and len(hist_data) >= 5:
                    data['historical_data'][code] = hist_data
                    success_count += 1

                if (idx + 1) % 10 == 0 or (idx + 1) == len(limit_up_df):
                    self.logger.info(f"  进度: {idx + 1}/{len(limit_up_df)}，成功获取 {success_count} 只股票历史数据")

            except Exception as e:
                self.logger.warning(f"获取 {code} 历史数据失败: {str(e)}")

        self.logger.info(f"历史数据获取完成: {success_count}/{len(limit_up_df)}")

        return data

    def _analyze_limit_up(self, market_data: Dict) -> Dict:
        """分析涨停板"""
        return self.limit_up_analyzer.analyze_limit_up(
            market_data['limit_up_df'],
            market_data['historical_data']
        )

    def _analyze_sectors(self, market_data: Dict) -> Dict:
        """分析板块效应"""
        return self.sector_analyzer.analyze_sectors(
            market_data['limit_up_df'],
            market_data['historical_data']
        )

    def _identify_roles(self, limit_up_analysis: Dict, sector_analysis: Dict) -> Dict:
        """识别龙头/中军/补涨"""
        return self.dragon_identifier.identify_roles(
            limit_up_analysis['stocks'],
            sector_analysis
        )

    def _generate_strategy(self, limit_up_analysis: Dict, roles: Dict, trade_date: str,
                           ai_analysis: Dict = None) -> Dict:
        strategy = self.strategy_generator.generate_strategy(
            limit_up_analysis,
            roles,
            ai_analysis  # 直接传递 AI 分析结果
        )

        # 添加元数据
        strategy['meta'] = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'trade_date': trade_date,
            'version': '1.0',
            'data_source': self.config['data_sources']['primary']
        }

        # 如果 AI 分析存在但未在 StrategyGenerator 中处理（比如某些字段需额外补充），可保留外部整合
        # 但此处已由 StrategyGenerator 内部完成，无需额外操作
        return strategy

    def _generate_empty_report(self, trade_date: str) -> Dict:
        """生成空报告"""
        empty_strategy = {
            'meta': {
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'trade_date': trade_date,
                'version': '1.0',
                'data_source': self.config['data_sources']['primary'],
                'empty_report': True
            },
            '市场概况': {
                '涨停家数': 0,
                '连板高度': 0,
                '封板成功率': '0%',
                '市场情绪': '冰点',
                '赚钱效应': '差'
            },
            '主线分析': [],
            '个股策略': [],
            '风险提示': [
                '当日无涨停股票，市场极度低迷',
                '建议空仓观望，等待市场回暖',
                '注意控制仓位，避免盲目抄底'
            ],
            '操作建议': [
                '空仓观望，等待市场出现明确信号',
                '关注市场量能变化，等待放量上涨',
                '可关注抗跌板块或个股，但不宜重仓'
            ]
        }

        return empty_strategy

    def _save_results(self, strategy: Dict, trade_date: str):
        """保存结果到文件"""
        results_dir = "results"
        os.makedirs(results_dir, exist_ok=True)

        date_str = trade_date.replace('-', '')

        # 保存JSON格式
        json_file = os.path.join(results_dir, f"strategy_{date_str}.json")
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(strategy, f, ensure_ascii=False, indent=2)
        self.logger.info(f"✓ 结果保存为JSON: {json_file}")

        # 保存Markdown格式
        md_file = os.path.join(results_dir, f"strategy_{date_str}.md")
        self._save_markdown_report(strategy, md_file)
        self.logger.info(f"✓ 结果保存为Markdown: {md_file}")

        # 保存简洁文本格式
        txt_file = os.path.join(results_dir, f"summary_{date_str}.txt")
        self._save_text_summary(strategy, txt_file)
        self.logger.info(f"✓ 摘要保存为文本: {txt_file}")

        # 更新最新报告链接
        latest_file = os.path.join(results_dir, "latest.md")
        with open(latest_file, 'w', encoding='utf-8') as f:
            f.write(f"# 最新复盘报告\n\n")
            f.write(f"**交易日**: {trade_date}\n\n")
            f.write(f"**生成时间**: {strategy['meta']['generated_at']}\n\n")
            f.write(f"**报告文件**: [strategy_{date_str}.md](strategy_{date_str}.md)\n")

        self.logger.info(f"✓ 最新报告链接已更新")

    def _save_markdown_report(self, strategy: Dict, filepath: str):
        """保存Markdown报告"""
        try:
            from src.notification.wechat_notifier import WechatNotifier
            notifier = WechatNotifier(self.config)
            markdown_content = notifier._format_strategy_to_markdown(strategy)

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
        except Exception as e:
            self.logger.warning(f"保存Markdown报告失败: {str(e)}")
            simple_md = self._generate_simple_markdown(strategy)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(simple_md)

    def _generate_simple_markdown(self, strategy: Dict) -> str:
        """生成简单的Markdown报告"""
        lines = []

        # 标题
        trade_date = strategy['meta']['trade_date']
        lines.append(f"# A股打板复盘报告 - {trade_date}")
        lines.append(f"*生成时间: {strategy['meta']['generated_at']}*")
        lines.append("")

        # 市场概况
        lines.append("## 📊 市场概况")
        market = strategy['市场概况']
        lines.append(f"- **涨停家数**: {market.get('涨停家数', 0)}家")
        lines.append(f"- **连板高度**: {market.get('连板高度', 0)}板")
        lines.append(f"- **市场情绪**: {market.get('市场情绪', 'N/A')}")
        lines.append(f"- **赚钱效应**: {market.get('赚钱效应', 'N/A')}")
        lines.append("")

        # 主线分析
        lines.append("## 🎯 主线分析")
        themes = strategy['主线分析']
        if themes:
            for i, theme in enumerate(themes, 1):
                lines.append(f"{i}. **{theme['板块名称']}**")
                lines.append(f"   - 涨停家数: {theme['涨停家数']}")
                lines.append(f"   - 强度评级: {theme['强度评级']}")
                lines.append(f"   - 持续性: {theme['持续性判断']}")
        else:
            lines.append("暂无明确主线")
        lines.append("")

        # AI深度分析（新增部分）
        if 'AI深度分析' in strategy:
            lines.append("## 🤖 AI深度分析")
            ai_analysis = strategy['AI深度分析']
            lines.append(f"**分析时间**: {ai_analysis.get('分析时间', '未知')}")
            lines.append(f"**分析摘要**: {ai_analysis.get('分析摘要', '暂无')}")
            lines.append("")

            for role in ['龙头', '中军', '补涨']:
                role_stocks = ai_analysis.get('梯队分析', {}).get(role, [])
                if role_stocks:
                    lines.append(f"### {role}股分析")
                    for stock in role_stocks:
                        lines.append(f"#### {stock['股票']}")
                        lines.append(f"- **涨停原因**: {stock.get('消息催化', ['暂无'])[0]}")
                        lines.append(f"- **分析摘要**: {stock.get('分析摘要', '暂无')}")
                        lines.append("")
            lines.append("")

        # 个股策略
        lines.append("## 🚀 个股策略")
        stocks = strategy['个股策略']
        if stocks:
            for stock in stocks[:10]:
                lines.append(f"### {stock['名称']} ({stock['代码']})")
                lines.append(f"- **角色**: {stock['角色']}")
                lines.append(f"- **策略**: {stock['策略类型']}")
                lines.append(f"- **建议**: {stock['操作建议']}")
                lines.append(f"- **止损**: {stock['止损位']}")
                lines.append(f"- **目标**: {stock['目标位']}")
                if '备注' in stock:
                    lines.append(f"- **备注**: {stock['备注']}")
                lines.append("")
        else:
            lines.append("暂无推荐个股")
            lines.append("")

        # 风险提示
        lines.append("## ⚠️ 风险提示")
        warnings = strategy['风险提示']
        if warnings:
            for warning in warnings:
                lines.append(f"- {warning}")
        else:
            lines.append("- 暂无特殊风险提示")
        lines.append("")

        # 操作建议
        lines.append("## 💡 操作建议")
        suggestions = strategy['操作建议']
        if suggestions:
            for suggestion in suggestions:
                lines.append(f"- {suggestion}")
        lines.append("")

        # 结尾
        lines.append("---")
        lines.append("*本报告由A股打板复盘系统自动生成，仅供参考，投资有风险，入市需谨慎。*")

        return "\n".join(lines)

    def _save_text_summary(self, strategy: Dict, filepath: str):
        """保存文本摘要"""
        trade_date = strategy['meta']['trade_date']

        summary = f"""
========================================
A股打板复盘摘要 - {trade_date}
========================================

📊 市场概况
  涨停家数: {strategy['市场概况'].get('涨停家数', 0)}
  连板高度: {strategy['市场概况'].get('连板高度', 0)}
  市场情绪: {strategy['市场概况'].get('市场情绪', 'N/A')}
  赚钱效应: {strategy['市场概况'].get('赚钱效应', 'N/A')}

🎯 主线板块: {len(strategy['主线分析'])}个

🤖 AI分析: {'已启用' if 'AI深度分析' in strategy else '未启用'}

🚀 推荐个股: {len(strategy['个股策略'])}只

⚠️ 风险提示: {len(strategy['风险提示'])}条

💡 操作建议: {len(strategy['操作建议'])}条

========================================
生成时间: {strategy['meta']['generated_at']}
========================================
"""

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(summary)

    def _send_notifications(self, strategy: Dict, trade_date: str):
        """发送通知"""
        wechat_enabled = self.config['wechat']['enable']

        if wechat_enabled and self.notifier:
            try:
                success = self.notifier.send_strategy_report(strategy)
                if success:
                    self.logger.info("✓ 微信通知发送成功")
                else:
                    self.logger.warning("✗ 微信通知发送失败")
            except Exception as e:
                self.logger.error(f"发送微信通知异常: {str(e)}")
        else:
            self.logger.info("ℹ️  微信通知未启用")

        self._show_local_notification(strategy, trade_date)

    def _show_local_notification(self, strategy: Dict, trade_date: str):
        """显示本地通知"""
        print("\n" + "="*60)
        print(f"A股打板复盘完成 - {trade_date}")
        print("="*60)

        market = strategy['市场概况']
        print(f"\n📊 市场概况:")
        print(f"   涨停家数: {market.get('涨停家数', 0)}家")
        print(f"   连板高度: {market.get('连板高度', 0)}板")
        print(f"   市场情绪: {market.get('市场情绪', 'N/A')}")
        print(f"   赚钱效应: {market.get('赚钱效应', 'N/A')}")

        # 显示AI分析信息
        if 'AI深度分析' in strategy:
            ai_analysis = strategy['AI深度分析']
            print(f"\n🤖 AI深度分析:")
            for role in ['龙头', '中军', '补涨']:
                role_stocks = ai_analysis.get('梯队分析', {}).get(role, [])
                if role_stocks:
                    print(f"   {role}股: {len(role_stocks)}只已分析")

        themes = strategy['主线分析']
        print(f"\n🎯 主线板块: {len(themes)}个")
        for i, theme in enumerate(themes[:3], 1):
            print(f"   {i}. {theme['板块名称']} ({theme['涨停家数']}只涨停)")

        stocks = strategy['个股策略']
        print(f"\n🚀 推荐个股: {len(stocks)}只")
        for i, stock in enumerate(stocks[:5], 1):
            print(f"   {i}. {stock['名称']} ({stock['代码']}) - {stock['角色']}")

        print(f"\n📁 报告已保存至 results/ 目录")
        print("="*60)

    def _send_error_notification(self, error_msg: str, trade_date: str = None):
        """发送错误通知"""
        if trade_date is None:
            trade_date = "unknown"

        error_summary = f"""
复盘系统运行失败！
日期: {trade_date}
时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
错误: {error_msg[:200]}
"""

        error_log_dir = "logs/errors"
        os.makedirs(error_log_dir, exist_ok=True)
        error_log_file = os.path.join(error_log_dir, f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

        with open(error_log_file, 'w', encoding='utf-8') as f:
            f.write(error_summary)
            import traceback
            f.write("\n\n详细错误信息:\n")
            f.write(traceback.format_exc())

        wechat_enabled = self.config['wechat']['enable']
        if wechat_enabled and self.notifier:
            try:
                self.notifier.send_error_notification(error_summary)
            except Exception as e:
                self.logger.error(f"发送错误通知失败: {str(e)}")

    def _record_run_stats(self, trade_date: str):
        """记录运行统计"""
        try:
            duration = 0
            if self.run_stats['start_time'] and self.run_stats['end_time']:
                duration = (self.run_stats['end_time'] - self.run_stats['start_time']).total_seconds()

            log_entry = {
                'trade_date': trade_date,
                'start_time': self.run_stats['start_time'].isoformat() if self.run_stats['start_time'] else None,
                'end_time': self.run_stats['end_time'].isoformat() if self.run_stats['end_time'] else None,
                'duration_seconds': duration,
                'success': self.run_stats['success'],
                'error': self.run_stats['error']
            }

            with open(self.run_log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

        except Exception as e:
            self.logger.warning(f"记录运行统计失败: {str(e)}")

    def run_quick_test(self):
        """运行快速测试"""
        print("=" * 60)
        print("A股打板复盘系统 - 快速测试模式")
        print("=" * 60)

        try:
            print(f"✓ 配置文件加载成功: {self.config_path}")
            print(f"  数据源主策略: {self.config['data_sources']['primary']}")
            print(f"  数据源备策略: {self.config['data_sources']['backup']}")
            print(f"  涨停阈值: {self.config['analysis']['涨停阈值']}%")

            print("\n🔍 检查系统组件状态...")
            if not hasattr(self, 'data_fetcher') or self.data_fetcher is None:
                print("❌ 错误: data_fetcher 组件未初始化")
                print("可能的原因:")
                print("  1. src/data/data_fetcher.py 文件不存在或语法错误")
                print("  2. 缺少必要的依赖包 (如 akshare)")
                print("  3. 配置文件格式错误")
                print("\n请检查以上问题后重试")
                return

            print("✅ 数据获取器组件就绪")

            print("\n📊 测试数据获取功能...")
            test_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')

            print(f"  正在获取股票基本信息...")
            stock_basic = self.data_fetcher.get_stock_basic()
            if not stock_basic.empty:
                print(f"  ✅ 股票基本信息: 获取到 {len(stock_basic)} 只股票")
                print(f"     示例: {stock_basic.iloc[0]['code']} - {stock_basic.iloc[0].get('name', 'N/A')}")
            else:
                print("  ❌ 股票基本信息: 获取失败")

            print(f"  正在获取上证指数日线数据...")
            test_code = "sh.000001"
            daily_data = self.data_fetcher.get_daily_data(test_code, test_date, test_date)
            if not daily_data.empty:
                print(f"  ✅ 日线数据: 成功获取 {test_code}")
                print(f"     日期: {test_date}, 收盘价: {daily_data.iloc[0].get('close', 'N/A')}")
            else:
                print(f"  ⚠️  日线数据: 获取 {test_code} 失败")

            print(f"  正在测试涨停数据获取...")
            limit_up_data = self.data_fetcher.get_today_limit_up(test_date)
            if not limit_up_data.empty:
                print(f"  ✅ 涨停数据: 获取到 {len(limit_up_data)} 只涨停股票")
                if len(limit_up_data) > 0:
                    print(f"     示例股票:")
                    for i in range(min(3, len(limit_up_data))):
                        stock = limit_up_data.iloc[i]
                        print(f"       {stock['code']} {stock.get('name', '')} 涨幅: {stock.get('pct_change', 0):.2f}%")
            else:
                print(f"  ⚠️  涨停数据: {test_date} 无涨停股票或获取失败")

            print("\n✅ 快速测试完成!")
            print("=" * 60)

        except Exception as e:
            print(f"\n❌ 快速测试失败: {str(e)}")
            import traceback
            traceback.print_exc()
            print("=" * 60)

def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='A股打板复盘系统')
    parser.add_argument('--date', type=str, help='指定交易日（格式: YYYY-MM-DD）')
    parser.add_argument('--test', action='store_true', help='运行快速测试')
    parser.add_argument('--config', type=str, help='指定配置文件路径')
    parser.add_argument('--no-notify', action='store_true', help='禁用通知')

    args = parser.parse_args()

    print("="*60)
    print("🚀 A股打板复盘系统 v1.0")
    print("="*60)

    try:
        system = StockReviewSystem(config_path=args.config)

        if args.test:
            system.run_quick_test()
            return

        if args.no_notify:
            system.config['wechat']['enable'] = False
            print("ℹ️  通知功能已禁用")

        system.run(trade_date=args.date)

    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断程序运行")
    except Exception as e:
        print(f"\n❌ 系统运行失败: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()