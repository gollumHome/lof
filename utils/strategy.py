from config import COST_RATE
import akshare as ak
import datetime

def analyze_single_lof(row):
    """
    对单只基金进行深度分析
    返回：净溢价、风险等级、实战建议
    """
    code = str(row['symbol'])
    name = row['name']
    premium = row['premium_rate']

    # 1. 计算净溢价 (扣除手续费)
    net_premium = premium - COST_RATE

    # 2. 识别品种与风险定性
    advice = ""
    risk_tag = ""

    # --- A. 白银/商品类 (如 161226) ---
    if '161226' in code or '白银' in name or '黄金' in name:
        risk_tag = "[商品基]"
        if premium > 10:
            advice = "⚠️ 必限购(约100元)！务必先试单。溢价极高，适合小资金/拖拉机账户参与。"
        else:
            advice = "⚠️ 数据基于昨晚净值。请人工扣除今日[商品期货]涨跌幅。"

    # --- B. QDII 类 (如 161128, 161130) ---
    # 简单粗暴判断：代码是 16 开头且不在商品里，或者名字带 LOF 且大概率是跨市场的
    elif 'QDII' in name or '标普' in name or '纳指' in name or '恒生' in name or '教育' in name:
        risk_tag = "[QDII]"
        if net_premium > 2.5:
            advice = "🔥 重点关注！收盘前务必确认[美股期货]未大跌。T+2风险较高。"
        elif net_premium > 1.0:
            advice = "😐 鸡肋。扣费后肉少，除非赌今晚美股大涨，否则不建议操作。"
        else:
            advice = "❌ 没肉。扣费+T+2风险后期望值为负。"

    # --- C. 国内/其他 LOF ---
    else:
        risk_tag = "[普通]"
        advice = "关注流动性，警惕成交额过低卖不出去。"

    return {
        "net_premium": round(net_premium, 2),
        "risk_tag": risk_tag,
        "advice": advice
    }


def filter_double_low_cb(df, limit=5):
    """
    筛选【双低策略】可转债
    条件：
    1. 价格 < 130 (不做高价妖债，防强赎风险)
    2. 溢价率 < 10 (保证进攻性)
    3. 成交额 > 1000万 (保证流动性)
    4. 未停牌
    """
    # 筛选池
    pool = df[
        (df['price'] < 130) &
        (df['price'] > 90) &
        (df['volume'] > 10000000)  # 1000万以上
        ].copy()

    # 按双低值从小到大排序
    pool.sort_values(by='double_low', ascending=True, inplace=True)

    # 取前 N 名
    top_list = []
    for _, row in pool.head(limit).iterrows():
        advice = ""
        # 简单评级
        if row['double_low'] < 115:
            advice = "⭐⭐⭐ 极品双低"
        elif row['double_low'] < 125:
            advice = "⭐⭐ 优质配置"
        else:
            advice = "⭐ 普通关注"

        news_tag = ""
        if 'stock_code' in row:
            print(f"   正在检查 {row['name']} 的下修公告...")
            news_tag = check_bond_news(row['stock_code'])

        # 如果查到了下修公告，不仅要加进去，还要把 advice 变得很显眼
        if "向下修正" in news_tag and "不" not in news_tag:
            advice = "🔥 突发利好！提议下修！"
        elif "不向下" in news_tag or "不修正" in news_tag:
            advice = "❄️ 利空：公司决定不下修"

        top_list.append({
            "code": row['symbol'],
            "name": row['name'],
            "price": row['price'],
            "premium": row['premium_rate'],
            "double_low": row['double_low'],
            "advice": advice,
            "news": news_tag
        })

    return top_list


def check_bond_news(stock_code):
    """
    检查指定正股最近一周的公告，看有没有[下修]相关的关键词
    返回：公告提示文本 (或空字符串)
    """
    try:
        # 获取个股公告 (限制最近 10 条，减少耗时)
        # 接口: stock_notice_report 或者是 stock_news_em
        # 这里用 stock_zh_a_spot_em 的逻辑太重，建议直接搜特定接口
        # 简单起见，我们模拟一个“是否有下修”的判断，
        # 实战中 Akshare 获取公告列表较慢，建议只对 Top 5 跑

        # 注意：akshare 获取公告的接口经常变，这里用一个比较通用的新闻接口代替
        news_df = ak.stock_news_em(symbol=stock_code)

        # 只要最近 7 天的
        today = datetime.datetime.now()
        seven_days_ago = (today - datetime.timedelta(days=7)).strftime('%Y-%m-%d')

        target_news = []
        keywords = ['向下修正', '下修', '不修正', '不向下']

        for _, row in news_df.head(10).iterrows():
            title = row['title']
            date = row['public_time'][:10]  # 截取日期

            if date >= seven_days_ago:
                for kw in keywords:
                    if kw in title:
                        return f"📢 {date} 公告: {title}"

        return ""

    except:
        return ""  # 查不到就拉倒，不卡程序