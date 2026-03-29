from data_engine import DataFetcher
from analysis_engine import AuditEngine
import pandas as pd
import numpy as np
import os
import json

def safe_get(df, cols):
    """稳健取数工具：支持多科目匹配，返回 Series"""
    if isinstance(cols, str): cols = [cols]
    for col in cols:
        if col in df.columns:
            return df[col].fillna(0)
    return pd.Series(0.0, index=df.index)

def save_for_web(report_df, stock_code):
    """
    【标准化路径版】确保在脚本所在目录下创建 data 文件夹
    """
    # 1. 获取当前 main.py 所在的绝对文件夹路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 2. 拼接出 data 文件夹的完整路径
    output_dir = os.path.join(current_dir, "data")
    
    # 3. 如果文件夹不存在则创建
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"📁 已在项目目录创建文件夹: {output_dir}")
    
    # 4. 导出 JSON
    json_path = os.path.join(output_dir, f"{stock_code}.json")
    report_df.to_json(json_path, orient='records', force_ascii=False, date_format='iso')
    print(f"✅ Web数据已就绪: {json_path}")

def run_investigation(stock_code):
    print(f"🚀 开始对股票 {stock_code} 进行财务审计...")
    
    # --- 1. 数据准备 ---
    fetcher = DataFetcher(stock_code)
    raw_files = fetcher.fetch_all_reports()
    engine = AuditEngine(fetcher.symbol, raw_files)
    df_bs_raw = engine.prepare_base() 
    
    if df_bs_raw is None: return

    # 时间轴对齐
    base_dates = engine.report_df[['报告日']]
    
    def load_and_align(file_key):
        df_raw = pd.read_excel(raw_files[file_key])
        df_raw['报告日'] = pd.to_datetime(df_raw['报告日'], format='%Y%m%d', errors='coerce')
        return pd.merge(base_dates, df_raw, on='报告日', how='left')

    df_bs = pd.merge(base_dates, df_bs_raw, on='报告日', how='left')
    df_is = load_and_align("利润表")
    df_cf = load_and_align("现金流量表")

    # ==========================================
    # 核心科目统一提取 (变量池)
    # ==========================================
    rev = safe_get(df_is, ['营业收入'])
    cost = safe_get(df_is, ['营业成本'])
    net_profit = safe_get(df_is, ['净利润'])
    total_profit = safe_get(df_is, ['利润总额'])
    sell_exp = safe_get(df_is, '销售费用')
    admin_exp = safe_get(df_is, '管理费用')
    fin_exp = safe_get(df_is, '财务费用')
    rd_exp = safe_get(df_is, '研发费用')
    asset_imp_loss = safe_get(df_is, '资产减值损失').abs()
    credit_imp_loss = safe_get(df_is, '信用减值损失').abs()
    net_profit_parent = safe_get(df_is, '归属于母公司所有者的净利润')
    ending_equity_parent = safe_get(df_bs, '归属于母公司股东权益合计')
    
    def get_avg(series):
        avg = (series + series.shift(1)) / 2
        return avg.where(series.shift(1) != 0, series)
    
    avg_equity_parent = get_avg(ending_equity_parent)
    op_profit = safe_get(df_is, '营业利润')
    revenue_growth_rate = rev.pct_change()
    op_profit_growth_rate = op_profit.pct_change()
    accumulated_depreciation = safe_get(df_bs, '累计折旧')
    current_depreciation = (accumulated_depreciation - accumulated_depreciation.shift(1)).abs()
    intangible_assets = safe_get(df_bs, '无形资产')
    long_term_deferred_exp = safe_get(df_bs, '长期待摊费用')
    amortization = ((intangible_assets.shift(1) - intangible_assets).clip(lower=0) + 
                    (long_term_deferred_exp.shift(1) - long_term_deferred_exp).clip(lower=0))
    
    invest_income = safe_get(df_is, '投资收益')
    fair_value_chg = safe_get(df_is, '公允价值变动收益')
    other_income = safe_get(df_is, '其他收益')
    asset_disposal_income = safe_get(df_is, '资产处置收益')
    
    total_assets = safe_get(df_bs, '资产总计')
    total_liab = safe_get(df_bs, '负债合计')
    curr_assets = safe_get(df_bs, '流动资产合计')
    curr_liab = safe_get(df_bs, '流动负债合计')
    cash = safe_get(df_bs, '货币资金')
    inventory = safe_get(df_bs, '存货')
    ar = safe_get(df_bs, '应收账款')
    goodwill = safe_get(df_bs, '商誉')
    ending_equity = safe_get(df_bs, '所有者权益(或股东权益)合计')
    fix_assets = safe_get(df_bs, ['固定资产净额', '固定资产净值', '固定资产'])
    
    ocf = safe_get(df_cf, '经营活动产生的现金流量净额')
    sales_cash_in = safe_get(df_cf, '销售商品、提供劳务收到的现金')
    cash_in_total = safe_get(df_cf, '经营活动现金流入小计')
    capex = safe_get(df_cf, ['购建固定资产、无形资产和其他长期资产支付的现金', '购建固定资产、无形资产和其他长期资产所支付的现金'])

    # ==========================================
    # 模块一：资产负债审计
    # ==========================================
    engine.add_indicator("资产负债率(%)", (total_liab / (total_assets + 0.1) * 100).round(2))
    engine.add_indicator("现金占比(%)", (cash / (total_assets + 0.1) * 100).round(2))
    engine.add_indicator("商誉占比(%)", (goodwill / (total_assets + 0.1) * 100).round(2))
    engine.add_indicator("应收增速超额(%)", (ar.pct_change() - rev.pct_change()).round(4) * 100)

    # ==========================================
    # 模块二：利润表审计
    # ==========================================
    engine.add_indicator("销售毛利率(%)", ((rev - cost) / (rev + 0.1) * 100).round(2))
    engine.add_indicator("销售净利率(%)", (net_profit / (rev + 0.1) * 100).round(2))
    approx_deduct_net_profit = op_profit - invest_income - fair_value_chg - other_income - asset_disposal_income
    engine.add_indicator("近似扣非净利润率(%)", (approx_deduct_net_profit / (rev + 0.1) * 100).round(2))

    # ==========================================
    # 模块三：现金流审计
    # ==========================================
    engine.add_indicator("净现比(倍)", (ocf / (net_profit + 0.1)).round(2))
    fcf = ocf - capex
    engine.add_indicator("自由现金流(元)", fcf.round(0))
    engine.add_indicator("自由现金流/净利润", (fcf / (net_profit.abs() + 0.1)).round(2))

    # ==========================================
    # 模块四：营运效率及杜邦分析
    # ==========================================
    avg_assets = get_avg(total_assets)
    avg_equity = get_avg(ending_equity)
    engine.add_indicator("总资产周转率(次)", (rev / (avg_assets + 0.1)).round(2))
    engine.add_indicator("权益乘数(杠杆倍数)", (avg_assets / (avg_equity + 0.1)).round(2))
    engine.add_indicator("净资产收益率(ROE/%)", (net_profit_parent / (avg_equity_parent + 0.1) * 100).round(2))

    # --- 保存结果 ---
    # 1. 保存本地 Excel (供你自己分析)
    engine.save_report()
    
    # 2. 【核心修改】保存 Web 端 JSON (供 GitHub Pages 托管)
    save_for_web(engine.report_df, stock_code)
    
    print(f"✨ {stock_code} 审计任务及 JSON 导出完成！")
    return engine.report_df

if __name__ == "__main__":
    # 这里填入你想要自动审计并上传的股票列表
    # 建议先放几个测试，跑通后再加全量
    task_list = ["600690", "002594", "01211"] 
    
    for code in task_list:
        try:
            run_investigation(code)
        except Exception as e:
            print(f"❌ 股票 {code} 处理失败: {e}")