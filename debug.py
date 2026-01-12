import akshare as ak
import pandas as pd

print("正在测试获取估值接口...")
try:
    df = ak.fund_value_estimation_em()
    print("\n====== 接口返回的真实列名 (请把下面这行发给我) ======")
    print(df.columns.tolist())
    print("==================================================\n")

    print("前一行数据样例:")
    print(df.head(1))
except Exception as e:
    print(f"接口报错: {e}")