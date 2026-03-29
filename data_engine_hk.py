import akshare as ak
import pandas as pd
import os

def fetch_and_save_hk_report(stock="00700"):
    print(f"🌐 正在抓取港股 {stock} 的年度财报数据...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # 创建专门存放港股原始数据的文件夹
    hk_data_dir = os.path.join(script_dir, "raw_data_hk")
    if not os.path.exists(hk_data_dir):
        os.makedirs(hk_data_dir)

    report_types = ["资产负债表", "利润表", "现金流量表"]
    
    for r_type in report_types:
        try:
            # 1. 调用接口获取长表数据
            df_raw = ak.stock_financial_hk_report_em(stock=stock, symbol=r_type, indicator="年度")
            
            if df_raw.empty:
                print(f"⚠️ {r_type} 未获取到数据")
                continue

            # 2. 核心转换：将长表透视为宽表
            # 我们需要：行(Index)是报告日，列(Columns)是科目名，值(Values)是金额
            df_pivot = df_raw.pivot_table(
                index="REPORT_DATE", 
                columns="STD_ITEM_NAME", 
                values="AMOUNT",
                aggfunc="first"
            ).reset_index()

            # 3. 统一日期格式和列名
            df_pivot = df_pivot.rename(columns={"REPORT_DATE": "报告日"})
            # 港股日期通常是 2023/12/31 格式，统一成 20231231 方便后续对齐
            df_pivot['报告日'] = pd.to_datetime(df_pivot['报告日']).dt.strftime('%Y%m%d')
            
            # 4. 排序：按日期降序排列
            df_pivot = df_pivot.sort_values(by="报告日", ascending=False)

            # 5. 保存 Excel
            file_name = f"{stock}_{r_type}.xlsx"
            save_path = os.path.join(hk_data_dir, file_name)
            df_pivot.to_excel(save_path, index=False)
            print(f"✅ 已保存: {save_path}")

        except Exception as e:
            print(f"❌ 抓取 {r_type} 时出错: {e}")

if __name__ == "__main__":
    fetch_and_save_hk_report("00700")