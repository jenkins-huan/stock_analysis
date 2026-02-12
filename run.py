#!/usr/bin/env python
"""
A股打板复盘系统 - 一键启动脚本
放在项目根目录
"""
import os
import sys
import argparse
from datetime import datetime

def check_project_structure():
    """检查项目结构"""
    print("🔍 检查项目结构...")
    
    required_files = [
        'config.yaml',
        'requirements.txt',
        'src/main.py',
        'src/data/data_fetcher.py',
        'src/analysis/limit_up_analyzer.py',
        'src/analysis/sector_analyzer.py',
        'src/analysis/dragon_head_identifier.py',
        'src/strategy/strategy_generator.py',
        'src/notification/wechat_notifier.py'
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ 缺少以下文件: {missing_files}")
        return False
    else:
        print("✅ 项目结构完整")
        return True

def check_config():
    """检查配置文件"""
    print("🔍 检查配置文件...")
    
    if not os.path.exists('config.yaml'):
        print("❌ 配置文件 config.yaml 不存在")
        
        # 询问是否创建默认配置
        response = input("是否创建默认配置文件？ (y/n): ")
        if response.lower() == 'y':
            create_default_config()
            return True
        else:
            return False
    
    try:
        import yaml
        with open('config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 检查必要配置项
        required_sections = ['data_sources', 'analysis', 'wechat']
        missing_sections = []
        
        for section in required_sections:
            if section not in config:
                missing_sections.append(section)
        
        if missing_sections:
            print(f"❌ 配置文件缺少必要章节: {missing_sections}")
            return False
        
        print("✅ 配置文件检查通过")
        return True
        
    except Exception as e:
        print(f"❌ 配置文件解析失败: {str(e)}")
        return False

def create_default_config():
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
   
# 微信机器人配置
wechat:
  webhook: "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=your_key_here"
  enable: false
  
# 运行配置
schedule:
  run_time: "18:00"
  market_days_only: true
"""
    
    with open('config.yaml', 'w', encoding='utf-8') as f:
        f.write(config_content)
    
    print("✅ 已创建默认配置文件: config.yaml")
    print("📝 请编辑此文件，配置你的 tushare token 等信息")

def install_dependencies():
    """安装依赖"""
    print("🔧 检查依赖...")
    
    try:
        import baostock
        import tushare
        import pandas
        import yaml
        print("✅ 所有依赖已安装")
        return True
    except ImportError as e:
        print(f"❌ 缺少依赖: {e}")
        
        if os.path.exists('requirements.txt'):
            response = input("是否自动安装依赖？ (y/n): ")
            if response.lower() == 'y':
                print("正在安装依赖，请稍候...")
                os.system('pip install -r requirements.txt')
                return True
        else:
            print("❌ 找不到 requirements.txt 文件")
            return False

def run_system(test_mode=False, date=None, no_notify=False):
    """运行复盘系统"""
    print("\n" + "="*60)
    print("🚀 启动A股打板复盘系统")
    print("="*60)
    
    try:
        # 将当前目录添加到路径
        sys.path.insert(0, os.getcwd())
        
        # 导入并运行主程序
        from src.main import main as run_main
        
        # 构建命令行参数
        args = []
        if test_mode:
            args.append('--test')
        if date:
            args.extend(['--date', date])
        if no_notify:
            args.append('--no-notify')
        
        # 设置命令行参数
        sys.argv = ['main.py'] + args
        
        # 运行主程序
        run_main()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断程序运行")
    except Exception as e:
        print(f"\n❌ 系统运行失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='A股打板复盘系统启动器')
    parser.add_argument('--test', action='store_true', help='运行快速测试模式')
    parser.add_argument('--date', type=str, help='指定交易日（格式: YYYY-MM-DD）')
    parser.add_argument('--no-notify', action='store_true', help='禁用微信通知')
    parser.add_argument('--check-only', action='store_true', help='仅检查项目结构，不运行')
    
    args = parser.parse_args()
    
    print("="*60)
    print("📊 A股打板复盘系统 v1.0")
    print("="*60)
    
    # 检查项目结构
    if not check_project_structure():
        return
    
    # 检查配置文件
    if not check_config():
        return
    
    # 安装依赖
    if not install_dependencies():
        return
    
    # 如果仅检查
    if args.check_only:
        print("\n✅ 项目检查完成，可以正常运行")
        return
    
    # 运行系统
    success = run_system(
        test_mode=args.test,
        date=args.date,
        no_notify=args.no_notify
    )
    
    if success:
        print("\n✅ 复盘系统运行完成")
    else:
        print("\n❌ 复盘系统运行失败")

if __name__ == "__main__":
    main()