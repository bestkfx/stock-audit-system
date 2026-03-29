import pandas as pd
import os

class AuditEngine:
    def __init__(self, stock_code, raw_paths):
        self.symbol = stock_code # 这里用原始输入或格式化后的均可
        self.raw_paths = raw_paths
        # 分析报告保存路径
        self.report_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{self.symbol}_财务分析报告.xlsx")
        self.report_df = pd.DataFrame()

    def prepare_base(self):
        """
        核心步骤：读取资产负债表底稿，建立以'报告日'为核心的时间轴。
        只筛选 12-31 的年报数据。
        """
        if not os.path.exists(self.raw_paths["资产负债表"]):
            return None
        
        df_bs = pd.read_excel(self.raw_paths["资产负债表"])
        # 转换日期格式
        df_bs['报告日'] = pd.to_datetime(df_bs['报告日'], format='%Y%m%d', errors='coerce')
        # 筛选年报并按时间升序排列
        df_bs = df_bs[df_bs['报告日'].dt.month == 12].sort_values('报告日')
        
        # 初始化最终的报告表：第一列是日期
        self.report_df = pd.DataFrame({'报告日': df_bs['报告日']})
        
        # 返回清洗后的原始资产负债表，供后续指标计算使用
        return df_bs

    def add_indicator(self, column_name, data_series):
        """
        通用扩展接口：将计算好的指标列追加到报告中。
        """
        # 确保数据长度一致
        self.report_df[column_name] = data_series.values
        print(f"📊 指标 [{column_name}] 已存入预备队列。")

    def save_report(self):
        """将内存中的分析结果写入 Excel"""
        self.report_df.to_excel(self.report_path, index=False)
        print(f"💾 最终分析报告已生成: {self.report_path}")