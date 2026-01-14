import datetime

import akshare as ak
import pandas as pd


def fetch_lof_data():
    """
    获取 LOF 实时数据（终极全覆盖版）
    逻辑：现价 + (优先用实时估值 else 用官方净值)
    """
    try:
        # ==========================================
        # 1. 获取行情价格 (Price)
        # ==========================================
        print("1. [正在获取] 行情价格 (fund_lof_spot_em)...")
        df_price = ak.fund_lof_spot_em()
        df_price.rename(columns={"代码": "symbol", "名称": "name", "最新价": "price", "成交额": "volume"}, inplace=True)
        df_price['symbol'] = df_price['symbol'].astype(str)
        # 过滤成交额太小的，但先保留白银LOF
        df_price = df_price[df_price['price'] > 0]

        # ==========================================
        # 2. 获取实时估值 (IOPV - 针对QDII/股票基)
        # ==========================================
        print("2. [正在获取] 实时估值 (fund_value_estimation_em)...")
        try:
            df_iopv = ak.fund_value_estimation_em()
            # 动态找列名
            code_col_iopv = next((c for c in df_iopv.columns if "代码" in c), None)
            val_col_iopv = next((c for c in df_iopv.columns if "估算值" in c or "实时估值" in c), None)

            if code_col_iopv and val_col_iopv:
                df_iopv = df_iopv[[code_col_iopv, val_col_iopv]]
                df_iopv.columns = ['symbol', 'iopv_realtime']  # 重命名
                df_iopv['symbol'] = df_iopv['symbol'].astype(str)
            else:
                df_iopv = pd.DataFrame(columns=['symbol', 'iopv_realtime'])
        except:
            print("   (实时估值接口获取失败或超时，将只使用官方净值)")
            df_iopv = pd.DataFrame(columns=['symbol', 'iopv_realtime'])

        # ==========================================
        # 3. 获取官方净值 (NAV - 针对白银/商品基)
        # ==========================================
        print("3. [正在获取] 官方净值 (fund_open_fund_rank_em)...")
        # 这个接口包含全市场所有基金的最新单位净值
        try:
            df_nav = ak.fund_open_fund_rank_em(symbol="全部")
            # 通常列名：['基金代码', '基金简称', ..., '单位净值', ...]
            # 同样动态找一下
            code_col_nav = next((c for c in df_nav.columns if "代码" in c), None)
            nav_col_nav = next((c for c in df_nav.columns if "单位净值" in c), None)
            date_col_nav = next((c for c in df_nav.columns if "日期" in c), None)

            if code_col_nav and nav_col_nav:
                df_nav = df_nav[[code_col_nav, nav_col_nav, date_col_nav]]
                df_nav.columns = ['symbol', 'nav_official', 'nav_date']
                df_nav['symbol'] = df_nav['symbol'].astype(str)
            else:
                df_nav = pd.DataFrame(columns=['symbol', 'nav_official'])
        except:
            print("   (官方净值接口异常)")
            df_nav = pd.DataFrame(columns=['symbol', 'nav_official'])

        # ==========================================
        # 4. 数据合并 (三表合一)
        # ==========================================
        print("4. [正在计算] 数据合并与溢价计算...")

        # 以 Price 表为主，左连接 IOPV 和 NAV
        df_final = pd.merge(df_price, df_iopv, on='symbol', how='left')
        df_final = pd.merge(df_final, df_nav, on='symbol', how='left')

        # ==========================================
        # 5. 核心逻辑：IOPV 选取策略
        # ==========================================
        # 逻辑：
        # 1. 如果有实时估值 (iopv_realtime)，就用实时的。
        # 2. 如果没有实时估值 (比如白银161226)，就用官方净值 (nav_official)。

        # 先转数字
        cols = ['price', 'iopv_realtime', 'nav_official', 'volume']
        for c in cols:
            if c in df_final.columns:
                df_final[c] = pd.to_numeric(df_final[c], errors='coerce')

        # 核心填充逻辑：创建一个最终的 'iopv' 列
        # 优先使用 iopv_realtime，如果为空(NaN)，则填充 nav_official
        df_final['iopv'] = df_final['iopv_realtime'].fillna(df_final['nav_official'])

        # 标记数据来源 (可选，方便调试)
        df_final['source'] = df_final.apply(
            lambda x: '实时估值' if pd.notnull(x['iopv_realtime']) else (
                '官方净值' if pd.notnull(x['nav_official']) else '无数据'),
            axis=1
        )

        # 清洗数据
        df_final.dropna(subset=['price', 'iopv'], inplace=True)
        df_final = df_final[df_final['iopv'] > 0.001]

        # 计算溢价率
        df_final['premium_rate'] = (df_final['price'] - df_final['iopv']) / df_final['iopv'] * 100

        # --- 特别调试：打印白银LOF的情况 ---
        silver_check = df_final[df_final['symbol'] == '161226']
        if not silver_check.empty:
            print(
                f"✅ 成功抓取白银LOF(161226): 现价={silver_check.iloc[0]['price']}, 参考净值={silver_check.iloc[0]['iopv']}, 溢价率={silver_check.iloc[0]['premium_rate']:.2f}%")
        else:
            print("⚠️ 警告：依然未找到白银LOF(161226)的数据，请检查代码列表。")

        return df_final

    except Exception as e:
        print(f"❌ 程序发生错误: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def fetch_cb_data():
    """
    获取可转债实时数据 (终极适配版)
    核心逻辑：
    1. 使用 bond_cov_comparison 接口 (含价格+溢价率)。
    2. 动态模糊匹配列名，防止 API 字段变动。
    3. 获取[正股代码]以便后续查询下修公告。
    4. 自动计算双低值。
    """
    try:
        print("📥 [正在获取] 可转债实时行情 (bond_cov_comparison)...")
        # 接口：东方财富-可转债比价表
        df = ak.bond_cov_comparison()

        # --- 1. 动态寻找关键列名 ---
        col_map = {}

        for col in df.columns:
            # 排除包含 "正股" 的列名混淆，除非是我们明确需要的 "正股代码"

            # 找转债代码
            if "代码" in col and "正股" not in col:
                col_map["symbol"] = col
            # 找转债名称
            elif "名称" in col and "正股" not in col:
                col_map["name"] = col
            # 找转债最新价
            elif "最新价" in col and "正股" not in col:
                col_map["price"] = col
            # 找转股溢价率
            elif "溢价率" in col:
                col_map["premium_rate"] = col
            # 找成交额 (可能叫 成交额 或 成交金额)
            elif "成交" in col or "金额" in col:
                col_map["volume"] = col
            # 找正股代码 (用于后续查公告)
            elif "正股代码" in col:
                col_map["stock_code"] = col

        # --- 2. 检查核心数据是否找到 ---
        if "price" not in col_map or "premium_rate" not in col_map:
            print(f"❌ 关键列(最新价/溢价率)丢失！当前所有列名: {df.columns.tolist()}")
            return pd.DataFrame()

        # --- 3. 重命名 ---
        # 将找到的列名 (Value) 映射为标准名 (Key)
        rename_dict = {v: k for k, v in col_map.items()}
        df.rename(columns=rename_dict, inplace=True)

        # --- 4. 数据清洗与兜底 ---

        # 兜底：如果接口里没返回成交额，默认给一个较大值，防止被策略过滤
        if 'volume' not in df.columns:
            # print("⚠️ 警告：该接口未返回成交额，默认视为流动性充足。")
            df['volume'] = 20000000

            # 兜底：如果没抓到正股代码，给个空字符串，防止报错
        if 'stock_code' not in df.columns:
            df['stock_code'] = ""

        # 类型转换 (强制转数字，非数字变NaN)
        numeric_cols = ['price', 'premium_rate', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 过滤无效数据
        # 1. 价格和溢价率不能为空
        df.dropna(subset=['price', 'premium_rate'], inplace=True)
        # 2. 价格必须大于0 (过滤停牌或未上市)
        df = df[df['price'] > 0]

        # --- 5. 计算双低 (Double Low) ---
        # 双低 = 价格 + 溢价率
        # 例: 价格110 + 溢价率5(%) = 115
        df['double_low'] = df['price'] + df['premium_rate']

        print(f"✅ 可转债数据获取成功，共 {len(df)} 条。")
        return df

    except Exception as e:
        print(f"❌ 可转债数据获取失败: {e}")
        # import traceback; traceback.print_exc() # 调试时可打开
        return pd.DataFrame()


def fetch_today_ipo():
    """
    获取今日可申购的新股和新债 (适配 Akshare 1.18.9 巨潮资讯接口)
    """
    today_date = datetime.datetime.now().strftime('%Y-%m-%d')
    # 调试用：你可以把日期改成一个已知有申购的日子来测试，例如 '2023-12-26'
    # today_date = '2023-12-26'

    ipo_data = {
        "stocks": [],
        "bonds": []
    }

    print(f"📅 正在检查今日 ({today_date}) 的申购机会...")

    # ==============================
    # 1. 获取新债 (CNINFO 巨潮资讯)
    # ==============================
    try:
        # 接口: 巨潮资讯-数据中心-专题统计-债券报表-债券发行-可转债发行
        # 对应你的版本 __init__.py 中存在的 bond_cov_issue_cninfo
        df_bond = ak.bond_cov_issue_cninfo()

        # 巨潮接口通常返回列：['债券代码', '债券简称', '申购日期', '申购代码', ...]
        if not df_bond.empty:
            # 统一日期格式
            date_col = '申购日期'
            if date_col in df_bond.columns:
                df_bond[date_col] = df_bond[date_col].astype(str)
                # 筛选今天
                today_bonds = df_bond[df_bond[date_col] == today_date]

                for _, row in today_bonds.iterrows():
                    ipo_data['bonds'].append({
                        "code": row.get('债券代码', 'N/A'),
                        "name": row.get('债券简称', 'N/A'),
                        "price": "100.00"
                    })
    except Exception as e:
        print(f"⚠️ 新债接口报错: {e}")

    # ==============================
    # 2. 获取新股 (CNINFO 巨潮资讯)
    # ==============================
    try:
        # 接口: 巨潮资讯-数据中心-新股数据-新股发行
        # 对应你的版本 __init__.py 中存在的 stock_new_ipo_cninfo
        df_stock = ak.stock_new_ipo_cninfo()

        # 巨潮接口通常返回列：['证券代码', '证券简称', '申购日期', '发行价', ...]
        if not df_stock.empty:
            date_col = '申购日期'
            if date_col in df_stock.columns:
                df_stock[date_col] = df_stock[date_col].astype(str)
                today_stocks = df_stock[df_stock[date_col] == today_date]

                for _, row in today_stocks.iterrows():
                    ipo_data['stocks'].append({
                        "code": row.get('证券代码', 'N/A'),
                        "name": row.get('证券简称', 'N/A'),
                        "price": str(row.get('发行价', '0'))
                    })
    except Exception as e:
        print(f"⚠️ 新股接口报错: {e}")

    count = len(ipo_data['stocks']) + len(ipo_data['bonds'])
    if count > 0:
        print(f"✅ 发现 {count} 个申购机会！")
    else:
        print("✅ 今日无申购。")

    return ipo_data


def fetch_repo_data():
    """
    获取国债逆回购实时数据 (GC001 和 R-001)
    修正版：分别获取沪深两市数据并合并
    """
    try:
        print("💰 [正在获取] 国债逆回购实时利率...")

        # 1. 获取上海市场 (GC系列)
        try:
            df_sh = ak.bond_sh_buy_back_em()
            # 筛选 GC001 (代码 204001)
            df_sh = df_sh[df_sh['代码'] == '204001'].copy()
        except Exception as e:
            print(f"   ⚠️ 上海逆回购接口报错: {e}")
            df_sh = pd.DataFrame()

        # 2. 获取深圳市场 (R-系列)
        try:
            df_sz = ak.bond_sz_buy_back_em()
            # 筛选 R-001 (代码 131810)
            df_sz = df_sz[df_sz['代码'] == '131810'].copy()
        except Exception as e:
            print(f"   ⚠️ 深圳逆回购接口报错: {e}")
            df_sz = pd.DataFrame()

        # 3. 合并数据
        if df_sh.empty and df_sz.empty:
            return pd.DataFrame()

        df = pd.concat([df_sh, df_sz], ignore_index=True)

        # 4. 数据清洗
        # 接口返回列名通常为: ['代码', '名称', '最新价', '涨跌幅', ...]
        # 最新价 即为 年化利率
        df.rename(columns={
            '代码': 'code',
            '名称': 'name',
            '最新价': 'rate',
            '涨跌幅': 'change_percent'
        }, inplace=True)

        # 确保是数字类型
        df['rate'] = pd.to_numeric(df['rate'], errors='coerce')

        return df

    except Exception as e:
        print(f"❌ 国债逆回购获取失败: {e}")
        return pd.DataFrame()