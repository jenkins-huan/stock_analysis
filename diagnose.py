#!/usr/bin/env python
"""
系统诊断脚本 - 定位组件初始化失败原因
"""
import os
import sys
import traceback
import yaml

def print_section(title):
    """打印章节标题"""
    print("\n" + "="*60)
    print(f"🔍 {title}")
    print("="*60)

def check_python_environment():
    """检查Python环境"""
    print_section("Python环境检查")
    print(f"Python版本: {sys.version}")
    print(f"工作目录: {os.getcwd()}")
    print(f"Python路径:")
    for path in sys.path[:5]:
        print(f"  {path}")

def check_project_structure():
    """检查项目结构"""
    print_section("项目结构检查")
    
    structure = {
        'config.yaml': '主配置文件',
        'src/': '源代码目录',
        'src/main.py': '主程序',
        'src/data/': '数据模块',
        'src/data/data_fetcher.py': '数据获取器',
        'src/analysis/': '分析模块',
        'src/analysis/limit_up_analyzer.py': '涨停分析器',
        'src/analysis/sector_analyzer.py': '板块分析器',
        'src/analysis/dragon_head_identifier.py': '龙头识别器',
        'src/strategy/': '策略模块',
        'src/strategy/strategy_generator.py': '策略生成器',
        'src/notification/': '通知模块',
        'src/notification/wechat_notifier.py': '微信通知器',
    }
    
    all_ok = True
    for path, desc in structure.items():
        exists = os.path.exists(path)
        status = "✅" if exists else "❌"
        print(f"{status} {desc:20} {path}")
        if not exists:
            all_ok = False
    
    return all_ok

def check_dependencies():
    """检查依赖包"""
    print_section("依赖包检查")
    
    deps = [
        ('baostock', '0.8.8', '主数据源'),
        ('akshare', '1.12.0', '备用数据源'),
        ('pandas', '1.3.0', '数据处理'),
        ('numpy', '1.21.0', '数值计算'),
        ('yaml', '6.0', '配置解析'),
        ('requests', '2.26.0', 'HTTP请求'),
    ]
    
    for package, min_version, purpose in deps:
        try:
            module = __import__(package)
            version = getattr(module, '__version__', '未知')
            status = "✅"
            print(f"{status} {package:15} {version:10} ({purpose})")
        except ImportError as e:
            status = "❌"
            print(f"{status} {package:15} 未安装        ({purpose})")
            print(f"     安装命令: pip install {package}")

def check_config_file():
    """检查配置文件"""
    print_section("配置文件检查")
    
    if not os.path.exists('config.yaml'):
        print("❌ config.yaml 文件不存在")
        return False
    
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        print("✅ 配置文件格式正确")
        
        # 检查关键配置
        required = [
            ('data_sources', dict),
            ('data_sources.primary', str),
            ('data_sources.backup', str),
            ('analysis', dict),
            ('analysis.涨停阈值', (int, float)),
            ('wechat', dict),
        ]
        
        for key, expected_type in required:
            try:
                # 处理嵌套键
                keys = key.split('.')
                value = config
                for k in keys:
                    value = value[k]
                
                if not isinstance(value, expected_type):
                    print(f"❌ 配置 {key}: 类型错误，期望 {expected_type}，实际 {type(value)}")
                    return False
                else:
                    print(f"✅ 配置 {key}: {value}")
            except KeyError:
                print(f"❌ 配置 {key}: 不存在")
                return False
        
        return True
        
    except yaml.YAMLError as e:
        print(f"❌ YAML解析错误: {e}")
        return False
    except Exception as e:
        print(f"❌ 配置文件读取错误: {e}")
        return False

def check_module_imports():
    """检查模块导入"""
    print_section("模块导入检查")
    
    # 添加项目根目录到路径
    project_root = os.path.dirname(os.path.abspath(__file__))
    src_path = os.path.join(project_root, 'src')
    
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    modules_to_check = [
        ('src.data.data_fetcher', 'DataFetcher', '数据获取器'),
        ('src.analysis.limit_up_analyzer', 'LimitUpAnalyzer', '涨停分析器'),
        ('src.analysis.sector_analyzer', 'SectorAnalyzer', '板块分析器'),
        ('src.analysis.dragon_head_identifier', 'DragonHeadIdentifier', '龙头识别器'),
        ('src.strategy.strategy_generator', 'StrategyGenerator', '策略生成器'),
        ('src.notification.wechat_notifier', 'WechatNotifier', '微信通知器'),
    ]
    
    all_ok = True
    for module_path, class_name, desc in modules_to_check:
        try:
            # 动态导入
            module = __import__(module_path, fromlist=[class_name])
            cls = getattr(module, class_name)
            print(f"✅ {desc}: 导入成功")
        except ImportError as e:
            print(f"❌ {desc}: 导入失败 - {str(e)}")
            print(f"   模块路径: {module_path}")
            all_ok = False
        except AttributeError as e:
            print(f"❌ {desc}: 类 {class_name} 不存在 - {str(e)}")
            all_ok = False
        except Exception as e:
            print(f"❌ {desc}: 未知错误 - {str(e)}")
            all_ok = False
    
    return all_ok

def check_data_fetcher_syntax():
    """检查data_fetcher.py语法"""
    print_section("DataFetcher语法检查")
    
    fetcher_path = 'src/data/data_fetcher.py'
    if not os.path.exists(fetcher_path):
        print("❌ data_fetcher.py 文件不存在")
        return False
    
    try:
        # 编译检查
        with open(fetcher_path, 'r', encoding='utf-8') as f:
            code = f.read()
        
        # 检查常见语法问题
        import ast
        tree = ast.parse(code, filename=fetcher_path)
        print("✅ 语法检查通过 (AST解析成功)")
        
        # 检查导入语句
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for name in node.names:
                    imports.append(name.name)
            elif isinstance(node, ast.ImportFrom):
                imports.append(f"{node.module}")
        
        print(f"  导入的模块: {', '.join(imports[:5])}...")
        
        return True
        
    except SyntaxError as e:
        print(f"❌ 语法错误: {e}")
        print(f"  文件: {e.filename}, 行: {e.lineno}, 列: {e.offset}")
        print(f"  错误文本: {e.text}")
        return False
    except Exception as e:
        print(f"❌ 检查失败: {e}")
        return False

def test_data_fetcher_instantiation():
    """测试DataFetcher实例化"""
    print_section("DataFetcher实例化测试")
    
    try:
        # 创建最小配置
        test_config = {
            'data_sources': {
                'primary': 'baostock',
                'backup': 'akshare'
            },
            'baostock': {
                'username': '',
                'password': ''
            },
            'akshare': {
                'enable': True,
                'timeout': 15
            },
            'analysis': {
                '涨停阈值': 9.8,
                '连板天数': 3,
                '板块强度阈值': 3,
                '龙头评分权重': {
                    '连板高度': 0.35,
                    '涨停时间': 0.25,
                    '封单金额': 0.20,
                    '流通市值': 0.20
                }
            },
            'wechat': {
                'webhook': '',
                'enable': False
            }
        }
        
        # 导入DataFetcher
        sys.path.insert(0, '.')
        from src.data.data_fetcher import DataFetcher
        
        print("正在实例化DataFetcher...")
        fetcher = DataFetcher(test_config)
        
        print("✅ DataFetcher实例化成功")
        
        # 测试基本方法
        print("\n测试DataFetcher方法:")
        
        # 测试get_stock_basic
        try:
            stocks = fetcher.get_stock_basic()
            if not stocks.empty:
                print(f"✅ get_stock_basic: 成功，获取 {len(stocks)} 只股票")
            else:
                print("⚠️  get_stock_basic: 返回空数据")
        except Exception as e:
            print(f"❌ get_stock_basic失败: {str(e)[:100]}")
        
        # 测试get_daily_data
        try:
            daily_data = fetcher.get_daily_data("000001", "2024-01-01", "2024-01-05")
            if not daily_data.empty:
                print(f"✅ get_daily_data: 成功，获取 {len(daily_data)} 条日线数据")
            else:
                print("⚠️  get_daily_data: 返回空数据")
        except Exception as e:
            print(f"❌ get_daily_data失败: {str(e)[:100]}")
        
        return True
        
    except ImportError as e:
        print(f"❌ 无法导入DataFetcher: {e}")
        return False
    except Exception as e:
        print(f"❌ DataFetcher实例化失败: {e}")
        print("错误详情:")
        traceback.print_exc()
        return False

def main():
    """主诊断函数"""
    print("="*70)
    print("🚀 A股打板复盘系统 - 深度诊断报告")
    print("="*70)
    
    # 检查Python环境
    check_python_environment()
    
    # 检查项目结构
    structure_ok = check_project_structure()
    
    # 检查依赖包
    check_dependencies()
    
    # 检查配置文件
    config_ok = check_config_file()
    
    # 检查模块导入
    imports_ok = check_module_imports()
    
    # 检查DataFetcher语法
    syntax_ok = check_data_fetcher_syntax()
    
    # 测试DataFetcher实例化（仅在基础检查通过后进行）
    if all([structure_ok, config_ok, imports_ok, syntax_ok]):
        fetcher_ok = test_data_fetcher_instantiation()
    else:
        print_section("跳过DataFetcher实例化测试（基础检查未通过）")
        fetcher_ok = False
    
    # 总结
    print_section("诊断总结")
    
    checks = {
        "项目结构": structure_ok,
        "配置文件": config_ok,
        "模块导入": imports_ok,
        "语法检查": syntax_ok,
        "DataFetcher实例化": fetcher_ok
    }
    
    passed = sum(checks.values())
    total = len(checks)
    
    print(f"✅ 通过: {passed}/{total}")
    
    for check_name, status in checks.items():
        symbol = "✅" if status else "❌"
        print(f"{symbol} {check_name}")
    
    if all(checks.values()):
        print("\n🎉 所有检查通过！系统应该可以正常运行。")
        print("运行命令: python src/main.py --test")
    else:
        print("\n⚠️  发现以下问题需要修复:")
        for check_name, status in checks.items():
            if not status:
                print(f"  - {check_name}")
        
        print("\n💡 建议修复步骤:")
        print("  1. 确保所有必需文件存在")
        print("  2. 安装缺失的依赖包: pip install -r requirements.txt")
        print("  3. 检查config.yaml格式是否正确")
        print("  4. 检查data_fetcher.py是否有语法错误")
    
    print("\n" + "="*70)

if __name__ == "__main__":
    main()