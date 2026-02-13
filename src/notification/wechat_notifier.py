"""
企业微信机器人通知模块
支持显示 DeepSeek AI 分析的涨停原因和消息催化
"""
import requests
import json
import logging
from typing import Dict, List, Any

class WechatNotifier:
    def __init__(self, config: Dict):
        self.config = config
        self.webhook_url = config['wechat']['webhook']
        self.enabled = config['wechat']['enable']
        self.logger = logging.getLogger(__name__)

    def send_strategy_report(self, strategy: Dict) -> bool:
        """发送策略报告"""
        if not self.enabled or not self.webhook_url:
            self.logger.warning("微信通知未启用或未配置webhook")
            return False

        try:
            markdown_content = self._format_strategy_to_markdown(strategy)

            message = {
                "msgtype": "markdown",
                "markdown": {
                    "content": markdown_content
                }
            }

            response = requests.post(
                self.webhook_url,
                json=message,
                timeout=10
            )

            if response.status_code == 200:
                self.logger.info("策略报告发送成功")
                return True
            else:
                self.logger.error(f"发送失败: {response.text}")
                return False

        except Exception as e:
            self.logger.error(f"发送微信通知异常: {str(e)}")
            return False

    def _format_strategy_to_markdown(self, strategy: Dict) -> str:
        """将策略格式化为Markdown（集成AI分析展示）"""
        lines = []

        # ----- 标题 -----
        trade_date = strategy['meta']['trade_date']
        lines.append(f"## 📊 A股打板复盘报告 - {trade_date}")
        lines.append(f"**生成时间**: {strategy['生成时间']}\n")

        # ----- 市场概况 -----
        lines.append("### 📈 市场概况")
        market = strategy['市场概况']
        lines.append(f"- **涨停家数**: {market.get('涨停家数', 0)}家")
        lines.append(f"- **连板高度**: {market.get('连板高度', 0)}板")
        lines.append(f"- **封板成功率**: {market.get('封板成功率', 'N/A')}")
        lines.append(f"- **市场情绪**: {market.get('市场情绪', 'N/A')}")
        lines.append(f"- **赚钱效应**: {market.get('赚钱效应', 'N/A')}\n")


        # 主线分析（AI 增强版）
        lines.append("### 🎯 主线分析")
        themes = strategy.get('主线分析', [])
        if themes:
            for i, theme in enumerate(themes, 1):
                lines.append(f"{i}. **{theme['板块名称']}**")
                lines.append(f"   - 涨停: {theme.get('涨停家数', 0)}家 | 强度: {theme.get('强度评级', 'N/A')}")
                lines.append(f"   - 持续性: {theme.get('持续性判断', 'N/A')}")

                # ----- AI 专属字段（简洁展示）-----
                if '龙头股' in theme and theme['龙头股']:
                    lines.append(f"   - 👑 龙头: {theme['龙头股']}")
                if '催化因素' in theme and theme['催化因素']:
                    cat = theme['催化因素'][:20] + ('...' if len(theme['催化因素']) > 20 else '')
                    lines.append(f"   - 🔥 催化: {cat}")
                if 'AI分析摘要' in theme and theme['AI分析摘要']:
                    abs_ = theme['AI分析摘要'][:30] + ('...' if len(theme['AI分析摘要']) > 30 else '')
                    lines.append(f"   - 💡 逻辑: {abs_}")
        else:
            lines.append("暂无明确主线\n")

        # ----- 个股策略（核心修改）-----
        lines.append("### 🚀 个股策略")
        stock_strategies = strategy['个股策略']
        if stock_strategies:
            for stock in stock_strategies[:5]:  # 最多显示5只
                lines.append(f"**{stock['名称']}** ({stock['代码']})")

                # 基础信息
                lines.append(f"- 角色: {stock.get('角色', 'N/A')}")
                if 'AI角色' in stock:
                    lines.append(f"- 🤖 AI确认角色: {stock['AI角色']}")
                lines.append(f"- 策略: {stock.get('策略类型', 'N/A')}")
                lines.append(f"- 建议: {stock.get('操作建议', 'N/A')}")
                lines.append(f"- 止损: {stock.get('止损位', 'N/A')}")
                lines.append(f"- 目标: {stock.get('目标位', 'N/A')}")

                # ===== 新增：显示涨停原因 / 消息催化 =====
                if '涨停原因' in stock and stock['涨停原因']:
                    lines.append(f"- **🚀 涨停原因/消息催化**:")
                    reasons = stock['涨停原因']
                    if isinstance(reasons, list):
                        # 最多显示3条，每条不超过50字符
                        for idx, reason in enumerate(reasons[:3]):
                            short_reason = reason[:50] + ('...' if len(reason) > 50 else '')
                            lines.append(f"  {idx+1}. {short_reason}")
                    else:
                        short_reason = str(reasons)[:50] + ('...' if len(str(reasons)) > 50 else '')
                        lines.append(f"  - {short_reason}")

                # ===== 新增：显示AI分析摘要 =====
                if 'AI分析摘要' in stock and stock['AI分析摘要']:
                    summary = stock['AI分析摘要']
                    short_summary = summary[:100] + ('...' if len(summary) > 100 else '')
                    lines.append(f"- **🤖 AI分析**: {short_summary}")

                # 备注
                if '备注' in stock:
                    lines.append(f"- 备注: {stock['备注']}")

                lines.append("")  # 空行分隔
        else:
            lines.append("暂无推荐个股\n")

        # ----- 风险提示 -----
        lines.append("### ⚠️ 风险提示")
        warnings = strategy['风险提示']
        if warnings:
            for warning in warnings:
                lines.append(f"- {warning}")
        else:
            lines.append("- 暂无特殊风险提示\n")

        # ----- 操作建议 -----
        lines.append("### 💡 操作建议")
        suggestions = strategy['操作建议']
        for suggestion in suggestions:
            lines.append(f"- {suggestion}")

        # ----- 尾部 -----
        lines.append("\n---")
        lines.append("**提示**: 以上为系统自动生成，仅供参考，投资需谨慎")

        return "\n".join(lines)

    def send_error_notification(self, error_msg: str) -> bool:
        """发送错误通知"""
        if not self.enabled:
            return False

        try:
            message = {
                "msgtype": "text",
                "text": {
                    "content": f"⚠️ 复盘系统运行异常\n{error_msg[:200]}",
                    "mentioned_list": ["@all"]
                }
            }

            response = requests.post(self.webhook_url, json=message, timeout=10)
            return response.status_code == 200

        except Exception:
            return False
