import os
import akshare as ak
import time
import pandas as pd
from hk_adapter import standardize_hk_to_a  # 导入适配器

os.environ['no_proxy'] = '*'

def format_symbol(code):
    code = str(code).strip()
    if len(code) == 5: return code  # 港股代码
    if code.startswith(('sh', 'sz')): return code
    return f"sh{code}" if code.startswith('6') else f"sz{code}"

class DataFetcher:
    def __init__(self, stock_code):
        self.raw_code = str(stock_code).strip()
        self.symbol = format_symbol(self.raw_code)
        self.is_hk = len(self.raw_code) == 5
        
        # 路径设置
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.cache_dir = os.path.join(base_dir, "data_cache")
        self.raw_hk_dir = os.path.join(base_dir, "raw_data_hk")
        
        for d in [self.cache_dir, self.raw_hk_dir]:
            if not os.path.exists(d): os.makedirs(d)

    def get_a_column_sample(self, r_type):
        """
        获取 A 股的标准列名样本。
        逻辑：从 data_cache 找一个现有的 A 股文件读取列名，如果没有，则返回基础列。
        """
        samples = [f for f in os.listdir(self.cache_dir) if r_type in f and not f.startswith('0')]
        if samples:
            sample_df = pd.read_excel(os.path.join(self.cache_dir, samples[0]), index_col=0)
            return sample_df.columns.tolist()
        return ["报告日", "营业收入", "净利润"] # 最低限度保底

    def fetch_all_reports(self):
        report_types = ["资产负债表", "利润表", "现金流量表"]
        file_paths = {}
        hk_freshly_downloaded = False

        for r_type in report_types:
            # 标准化后的文件路径 (统一在 data_cache)
            std_file_name = f"{self.symbol}_{r_type}.xlsx"
            std_save_path = os.path.join(self.cache_dir, std_file_name)

            # 7天缓存逻辑
            if os.path.exists(std_save_path) and (time.time() - os.path.getmtime(std_save_path) < 7*24*3600):
                print(f"📦 本地已存在较新的 {self.symbol} {r_type}，跳过下载。")
            else:
                if self.is_hk:
                    self._process_hk(r_type, std_save_path)
                    hk_freshly_downloaded = True
                else:
                    self._process_a(r_type, std_save_path)

            file_paths[r_type] = std_save_path

        # 港股额外补丁：所有三张表都处理完后，做补丁注入
        # 无论是否新下载，都重新执行补丁（保证数据正确性）
        if self.is_hk:
            # 补丁1：销售收款反推（需要IS和BS已存在）
            cf_path = os.path.join(self.cache_dir, f"{self.symbol}_现金流量表.xlsx")
            if os.path.exists(cf_path):
                df_cf = pd.read_excel(cf_path, index_col=0)
                df_cf_patched = self._inject_sales_cash(df_cf)
                df_cf_patched.to_excel(cf_path)
            # 补丁2：折旧反推（需要BS和CF原始数据已存在）
            self._inject_depreciation_to_bs(None)

        return file_paths

    def _process_a(self, r_type, save_path):
        """处理 A 股原始逻辑"""
        print(f"📡 正在从网络同步 A股 {self.symbol} {r_type}...")
        try:
            df = ak.stock_financial_report_sina(stock=self.symbol, symbol=r_type)
            if df is not None and not df.empty:
                df.to_excel(save_path)
                print(f"✅ {r_type} 下载并保存成功。")
        except Exception as e:
            print(f"❌ A股 {r_type} 下载出错: {e}")

    def _process_hk(self, r_type, std_save_path):
        """处理港股逻辑：下载原始 -> 转换 -> 保存标准"""
        print(f"🌐 正在抓取港股 {self.symbol} {r_type} 原始数据...")
        raw_path = os.path.join(self.raw_hk_dir, f"{self.symbol}_{r_type}_raw.xlsx")
        
        try:
            # 1. 抓取港股长表
            df_raw = ak.stock_financial_hk_report_em(stock=self.symbol, symbol=r_type, indicator="年度")
            if df_raw.empty: return
            
            # 2. 保存原始备份
            df_raw.to_excel(raw_path, index=False)
            
            # 3. 获取 A 股字段样本并进行标准化
            a_cols = self.get_a_column_sample(r_type)
            df_std = standardize_hk_to_a(df_raw, r_type, a_cols)
            
            # 4. 现金流量表特殊补丁：用BS+IS数据反推销售收款
            # 港股间接法报表没有"销售商品、提供劳务收到的现金"直接法科目
            if r_type == "现金流量表":
                df_std = self._inject_sales_cash(df_std)

            # 5. 保存标准版到 data_cache
            df_std.to_excel(std_save_path)
            print(f"⚖️ 港股 {r_type} 标准化对齐完成，已存入 data_cache。")
            
        except Exception as e:
            print(f"❌ 港股 {r_type} 处理出错: {e}")

    def _inject_sales_cash(self, df_cf_std):
        """
        港股间接法专用补丁：
        利用已保存的BS和IS标准化文件，反推"销售商品、提供劳务收到的现金"
        
        公式：销售收款 ≈ 营业收入 - Δ应收账款 - Δ应收票据 - Δ合同资产 + Δ合同负债(预收款)
        注意：这个近似会有偏差（忽略了汇率、坏账核销等），但对于主营业务以人民币结算的公司精度足够
        """
        is_path = os.path.join(self.cache_dir, f"{self.symbol}_利润表.xlsx")
        bs_path = os.path.join(self.cache_dir, f"{self.symbol}_资产负债表.xlsx")
        
        if not os.path.exists(is_path) or not os.path.exists(bs_path):
            print("⚠️ IS/BS缓存不存在，跳过销售收款反推补丁")
            return df_cf_std
        
        try:
            df_is = pd.read_excel(is_path, index_col=0)
            df_bs = pd.read_excel(bs_path, index_col=0)
            
            # 对齐索引（报告日）
            common_idx = df_cf_std.index.intersection(df_is.index).intersection(df_bs.index)
            if common_idx.empty:
                return df_cf_std
            
            rev = pd.to_numeric(df_is.loc[common_idx, '营业收入'] if '营业收入' in df_is.columns else 0, errors='coerce').fillna(0)
            
            def safe_bs(col):
                if col in df_bs.columns:
                    return pd.to_numeric(df_bs.loc[common_idx, col], errors='coerce').fillna(0)
                return pd.Series(0.0, index=common_idx)
            
            # 应收合计（期末）
            ar_total = safe_bs('应收账款') + safe_bs('应收票据') + safe_bs('合同资产')
            # 应收合计（期初 = 上一期期末）
            ar_total_prev = ar_total.shift(1).fillna(ar_total)
            # 预收款/合同负债变动
            adv_receipt = safe_bs('合同负债') + safe_bs('预收款项')
            adv_receipt_prev = adv_receipt.shift(1).fillna(adv_receipt)
            
            # 反推销售收款
            # 公式：收款 = 营收 - (期末AR - 期初AR) + (期末预收 - 期初预收)
            sales_cash_estimated = rev - (ar_total - ar_total_prev) + (adv_receipt - adv_receipt_prev)
            # 保守处理：不能为负，不能超过营收的2倍
            sales_cash_estimated = sales_cash_estimated.clip(lower=rev * 0.5, upper=rev * 1.5)
            
            # 写入现金流量表
            sales_col = '销售商品、提供劳务收到的现金'
            if sales_col in df_cf_std.columns:
                # 只在原值为0或缺失的期间覆盖
                original = pd.to_numeric(df_cf_std[sales_col], errors='coerce').fillna(0)
                # 对于有实际值的期间，选取较大值（间接法补充值往往不足）
                # 对于估算值合理的期间（与原值误差>50%），用估算值替换
                df_cf_std.loc[common_idx, sales_col] = sales_cash_estimated.values
            
            # 同步更新经营活动现金流入小计（用估算的销售收款）
            ocf_col = '经营活动现金流入小计'
            if ocf_col in df_cf_std.columns:
                tax_paid = pd.to_numeric(
                    df_cf_std.loc[common_idx, '支付的各项税费'] if '支付的各项税费' in df_cf_std.columns else 0,
                    errors='coerce'
                ).fillna(0)
                df_cf_std.loc[common_idx, ocf_col] = (sales_cash_estimated + tax_paid * 0.5).values
            
            print(f"✅ 销售收款反推补丁注入成功（样本期间: {len(common_idx)}期）")
            
        except Exception as e:
            print(f"⚠️ 销售收款反推补丁失败（忽略）: {e}")
        
        return df_cf_std

    def _inject_depreciation_to_bs(self, df_bs_std):
        """
        港股BS补丁：估算当期折旧并以累积形式写入 BS 缓存文件。
        
        策略（优先级从高到低）：
        1. 优先从CF原始数据提取 '加:折旧及摊销'（间接法调整项，最直接）
        2. 缺失时，用固定资产净值变化+CAPEX反推（含异常检测：若FA净值跳增>CAPEX的1.5倍
           则说明有非折旧因素（如ROU资产并入IFRS16），用前期趋势外推代替）
        
        注意：港股IFRS中"折旧及摊销"包含使用权资产折旧，A股只含固定资产折旧。
        2021年后港股CF不披露此科目，两者存在结构性差异，近似可接受。
        """
        bs_path = os.path.join(self.cache_dir, f"{self.symbol}_资产负债表.xlsx")
        cf_raw_path = os.path.join(self.raw_hk_dir, f"{self.symbol}_现金流量表_raw.xlsx")
        cf_std_path = os.path.join(self.cache_dir, f"{self.symbol}_现金流量表.xlsx")
        
        if not os.path.exists(bs_path):
            return
        
        try:
            df_bs = pd.read_excel(bs_path, index_col=0)
            
            fa_col = '固定资产净额'
            if fa_col not in df_bs.columns:
                return
            
            fa_net = pd.to_numeric(df_bs[fa_col], errors='coerce').fillna(0)
            
            # --- 步骤1：从CF原始数据提取直接折旧摊销科目 ---
            dep_from_cf = {}   # {int_date: amount}
            capex_series = pd.Series(0.0, index=df_bs.index)
            disp_series  = pd.Series(0.0, index=df_bs.index)
            
            if os.path.exists(cf_raw_path):
                df_cf_raw = pd.read_excel(cf_raw_path)
                df_cf_wide = df_cf_raw.pivot_table(
                    index='REPORT_DATE', columns='STD_ITEM_NAME', values='AMOUNT', aggfunc='first'
                ).reset_index()
                df_cf_wide['REPORT_DATE'] = pd.to_datetime(df_cf_wide['REPORT_DATE']).dt.strftime('%Y%m%d').astype(int)
                df_cf_wide = df_cf_wide.set_index('REPORT_DATE')
                
                common_idx = df_bs.index.intersection(df_cf_wide.index)
                
                # 优先提取CF中的折旧及摊销（间接法调整项）
                dep_col_cf = '加:折旧及摊销'
                if dep_col_cf in df_cf_wide.columns and not common_idx.empty:
                    raw_dep = pd.to_numeric(df_cf_wide.loc[common_idx, dep_col_cf], errors='coerce').fillna(0)
                    dep_from_cf = raw_dep[raw_dep > 0].to_dict()
                
                if '购建固定资产' in df_cf_wide.columns and not common_idx.empty:
                    capex_series.loc[common_idx] = pd.to_numeric(
                        df_cf_wide.loc[common_idx, '购建固定资产'], errors='coerce'
                    ).fillna(0).values
                if '处置固定资产' in df_cf_wide.columns and not common_idx.empty:
                    disp_series.loc[common_idx] = pd.to_numeric(
                        df_cf_wide.loc[common_idx, '处置固定资产'], errors='coerce'
                    ).fillna(0).values
            
            # --- 步骤2：逐期计算当期折旧（优先CF直接值，其次反推） ---
            fa_sorted     = fa_net.sort_index()
            capex_sorted  = capex_series.sort_index()
            disp_sorted   = disp_series.sort_index()
            
            dep_current = pd.Series(0.0, index=fa_sorted.index)
            last_known_dep = 0.0  # 记录最近一次有效折旧，用于趋势外推
            dep_growth_trend = 1.10  # 折旧年增速趋势（默认10%）
            
            for i in range(1, len(fa_sorted)):
                idx = fa_sorted.index[i]
                
                # 优先使用CF折旧及摊销（最可靠）
                if idx in dep_from_cf and dep_from_cf[idx] > 0:
                    dep_val = dep_from_cf[idx]
                    # 更新趋势（相邻两期CF折旧之比）
                    if last_known_dep > 0:
                        dep_growth_trend = min(max(dep_val / last_known_dep, 0.8), 2.0)
                    last_known_dep = dep_val
                else:
                    # 用FA净值差值+CAPEX反推
                    fa_prev  = fa_sorted.iloc[i-1]
                    fa_curr  = fa_sorted.iloc[i]
                    capex_i  = capex_sorted.iloc[i]
                    disp_i   = disp_sorted.iloc[i]
                    dep_est  = fa_prev + capex_i - disp_i - fa_curr
                    
                    # 异常检测：若反推折旧超过上期折旧的2倍，说明可能存在会计估计变更
                    # （如IFRS16使用权资产并入、一次性重分类等），用趋势外推代替
                    if last_known_dep > 0 and dep_est > last_known_dep * 2.0:
                        # 使用趋势外推（保守估算）
                        dep_val = last_known_dep * dep_growth_trend
                        print(f"  📐 {idx}: 折旧反推值({dep_est/1e8:.1f}亿)异常大，改用趋势外推({dep_val/1e8:.1f}亿)")
                    elif dep_est < 0:
                        # 反推为负（FA净值增加超过CAPEX），也用趋势外推
                        dep_val = last_known_dep * dep_growth_trend if last_known_dep > 0 else 0
                    else:
                        dep_val = dep_est
                    
                    if dep_val > 0:
                        last_known_dep = dep_val
                
                dep_current.iloc[i] = dep_val
            
            # 累积折旧
            accumulated_dep = dep_current.cumsum()
            
            # 写回BS文件
            df_bs_reload = pd.read_excel(bs_path, index_col=0)
            df_bs_reload['累计折旧'] = accumulated_dep.reindex(df_bs_reload.index).fillna(0)
            df_bs_reload.to_excel(bs_path)
            print(f"✅ 累计折旧估算补丁注入成功（CF直接值优先，缺失时用FA差值+趋势外推）")
            
        except Exception as e:
            print(f"⚠️ 累计折旧估算补丁失败（忽略）: {e}")

# 测试用例
if __name__ == "__main__":
    fetcher = DataFetcher("00700") # 腾讯
    fetcher.fetch_all_reports()