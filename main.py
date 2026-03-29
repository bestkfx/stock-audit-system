from data_engine import DataFetcher
from analysis_engine import AuditEngine
import pandas as pd
import numpy as np
import os
import json
import sys  # 务必在文件顶部加入 import sys

def safe_get(df, cols):
    """稳健取数工具：支持多科目匹配，返回 Series"""
    if isinstance(cols, str): cols = [cols]
    for col in cols:
        if col in df.columns:
            return df[col].fillna(0)
    return pd.Series(0.0, index=df.index)

def save_for_web(report_df, stock_code):
    """
    【核心功能】将全量审计结果保存为 JSON，路径标准化
    """
    # 动态获取当前脚本所在目录，确保在 GitHub 环境下不跑偏
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, "data")
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 复制一份数据进行 JSON 转换
    web_df = report_df.copy()
    
    # 港股屏蔽逻辑预留
    is_hk = len(str(stock_code)) == 5
    if is_hk:
        pass # 后续在这里添加字段过滤逻辑
    
    json_path = os.path.join(output_dir, f"{stock_code}.json")
    # orient='records' 是前端表格最喜欢的格式
    web_df.to_json(json_path, orient='records', force_ascii=False, date_format='iso')
    print(f"✅ JSON 数据已就绪: {json_path}")

def run_investigation(stock_code):
    print(f"🚀 开始对股票 {stock_code} 进行全量财务审计...")
    
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
    # 变量提取池 (保留你所有的核心计算逻辑)
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
    rev_growth_amount = rev.diff()
    last_year_capex = capex.shift(1)
    total_equity = safe_get(df_bs, ['归属于母公司股东权益合计', '所有者权益(或股东权益)合计'])
    ap = safe_get(df_bs, '应付账款')
    np_payable = safe_get(df_bs, '应付票据')
    contract_liab = safe_get(df_bs, ['合同负债', '预收款项'])
    nr = safe_get(df_bs, '应收票据')
    prepayments = safe_get(df_bs, '预付款项')

    # ==========================================
    # 模块一：资产负债审计 (全量指标)
    # ==========================================
    engine.add_indicator("资产负债率(%)", (total_liab / (total_assets + 0.1) * 100).round(2))
    engine.add_indicator("现金占比(%)", (cash / (total_assets + 0.1) * 100).round(2))
    engine.add_indicator("流动比率", (curr_assets / (curr_liab + 0.1)).round(2))
    engine.add_indicator("速动比率", ((curr_assets - inventory) / (curr_liab + 0.1)).round(2))
    engine.add_indicator("商誉占比(%)", (goodwill / (total_assets + 0.1) * 100).round(2))
    engine.add_indicator("坏账风险强度(%)", (credit_imp_loss / (ar + 0.1) * 100).round(2))
    engine.add_indicator("存货减值压力(%)", (asset_imp_loss / (inventory + 0.1) * 100).round(2))
    engine.add_indicator("应收增速超额(%)", (ar.pct_change() - rev.pct_change()).round(4) * 100)
    engine.add_indicator("存货堆积超额(%)", (inventory.pct_change() - cost.pct_change()).round(4) * 100)
    ib_debt = safe_get(df_bs, '短期借款') + safe_get(df_bs, '一年内到期的非流动负债') + \
              safe_get(df_bs, '长期借款') + safe_get(df_bs, '应付债券')
    engine.add_indicator("有息负债率(%)", (ib_debt / (total_assets + 0.1) * 100).round(2))

    # ==========================================
    # 模块二：利润表审计 (全量指标)
    # ==========================================
    engine.add_indicator("销售毛利率(%)", ((rev - cost) / (rev + 0.1) * 100).round(2))
    engine.add_indicator("销售净利率(%)", (net_profit / (rev + 0.1) * 100).round(2))
    engine.add_indicator("减值/利润总额(%)", (asset_imp_loss / (total_profit.abs() + 0.1) * 100).round(2))
    approx_deduct_net_profit = op_profit - invest_income - fair_value_chg - other_income - asset_disposal_income
    engine.add_indicator("近似扣非净利润率(%)", (approx_deduct_net_profit / (rev + 0.1) * 100).round(2))
    for label, val in [("销售", sell_exp), ("管理", admin_exp), ("财务", fin_exp), ("研发", rd_exp)]:
        engine.add_indicator(f"{label}费用率(%)", (val / (rev + 0.1) * 100).round(2))
    total_dep_and_amort = current_depreciation + amortization
    engine.add_indicator("折旧补偿比", (total_dep_and_amort / (op_profit.abs() + 0.1)).round(4))
    dol = (op_profit_growth_rate / (revenue_growth_rate + 0.0001)).round(2)
    engine.add_indicator("经营杠杆系数(DOL)", dol.replace([np.inf, -np.inf], 0).fillna(0))

    # ==========================================
    # 模块三：现金流审计 (全量指标)
    # ==========================================
    engine.add_indicator("净现比(倍)", (ocf / (net_profit + 0.1)).round(2))
    engine.add_indicator("收现率(%)", (sales_cash_in / (rev + 0.1) * 100).round(2))
    engine.add_indicator("销售付现比(%)", (cash_in_total / (rev + 0.1) * 100).round(2))
    fcf = ocf - capex
    engine.add_indicator("自由现金流(元)", fcf.round(0))
    engine.add_indicator("自由现金流/净利润", (fcf / (net_profit.abs() + 0.1)).round(2))
    engine.add_indicator("资本开支/净利润", (capex / (net_profit.abs() + 0.1)).round(2).replace([np.inf, -np.inf], 0).fillna(0))
    engine.add_indicator("资本支出/折旧摊销", (capex / (total_dep_and_amort + 0.1)).round(2))
    engine.add_indicator("超额投资利润占比", ((capex - total_dep_and_amort) / (net_profit.abs() + 0.1)).round(4))

    # ==========================================
    # 模块四：营运效率及杜邦分析 (全量指标)
    # ==========================================
    avg_assets = get_avg(total_assets)
    avg_equity = get_avg(ending_equity)
    avg_ar = get_avg(ar)
    avg_inv = get_avg(inventory)
    engine.add_indicator("应收账款周转天数", (365 / (rev / (avg_ar + 0.1) + 0.001)).round(2))
    engine.add_indicator("存货周转天数", (365 / (cost / (avg_inv + 0.1) + 0.001)).round(2))
    engine.add_indicator("固定资产周转率(次)", (rev / (fix_assets + 0.1)).round(2))
    engine.add_indicator("总资产周转率(次)", (rev / (avg_assets + 0.1)).round(2))
    engine.add_indicator("权益乘数(杠杆倍数)", (avg_assets / (avg_equity + 0.1)).round(2))
    engine.add_indicator("净资产收益率(ROE/%)", (net_profit_parent / (avg_equity_parent + 0.1) * 100).round(2))
    asset_marginal_contribution = (rev_growth_amount / (last_year_capex + 0.1)).round(2)
    engine.add_indicator("资产边际贡献(营收/投入)", asset_marginal_contribution.replace([np.inf, -np.inf], 0).fillna(0))
    tangible_equity = total_equity - goodwill - intangible_assets
    rota = (net_profit / (tangible_equity.abs() + 0.1) * 100).round(2)
    engine.add_indicator("有形净资产收益率(%)", rota.replace([np.inf, -np.inf], 0).fillna(0))
    engine.add_indicator("产业链资金占用(元)", (ap + np_payable + contract_liab) - (ar + nr + prepayments))

    # --- 保存结果 ---
    engine.save_report() # 生成 Excel
    save_for_web(engine.report_df, stock_code) # 生成 JSON
    print(f"✨ {stock_code} 审计任务完成！")
    return engine.report_df.copy()

if __name__ == "__main__":
    # 获取命令行参数
    # sys.argv[0] 是脚本名，sys.argv[1] 是我们传进去的代码
    if len(sys.argv) > 1:
        # 如果 Actions 传了参数进来，就只跑这个代码
        target_code = sys.argv[1]
        print(f"接到手动指令，开始审计: {target_code}")
        run_investigation(target_code)
    else:
        # 如果没有参数（比如本地直接运行），跑默认列表
        default_list = ["600690", "002594"] 
        for code in default_list:
            run_investigation(code)
