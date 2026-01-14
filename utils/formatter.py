import datetime

from tabulate import tabulate
from utils.strategy import analyze_single_lof
from config import COST_RATE


def format_text_report(lof_df, lof_opps, cb_opps=None, ipo_data=None, repo_list=None): # <--- 新增 repo_list

    """
    生成纯文本推送报告
    包含:
    1. LOF 高价值机会详情
    2. LOF 全市场 Top 10
    3. 可转债双低策略 Top 5 (新增)
    """
    lines = []

    # ==============================
    # 📅 第一部分：今日打新 (新增，优先级最高)
    # ==============================
    if ipo_data and (ipo_data['stocks'] or ipo_data['bonds']):
        lines.append("📅 【今日打新提醒】")
        lines.append("💡 坚持申购，中签就是捡钱！")
        lines.append("-" * 30)

        # 1. 新债
        if ipo_data['bonds']:
            for item in ipo_data['bonds']:
                lines.append(f"🎁 [新债] {item['name']} ({item['code']})")
                lines.append(f"   申购建议: 顶格申购！(无风险)")
            if ipo_data['stocks']: lines.append("- - - -")  # 分割线

        # 2. 新股
        if ipo_data['stocks']:
            for item in ipo_data['stocks']:
                price_float = float(item['price']) if item['price'] else 0
                lines.append(f"🎰 [新股] {item['name']} ({item['code']})")
                lines.append(f"   发行价: {item['price']}元")

                # 简单的新股风控提示
                if price_float > 50:
                    lines.append("   ⚠️ 提示: 高价新股，注意破发风险！")
                elif item['name'].startswith('C') or item['name'].startswith('N'):
                    lines.append("   ⚠️ 提示: 前5日无涨跌幅限制，波动极大。")
                else:
                    lines.append("   建议: 积极申购")

        lines.append("\n")  # 空一行
    else:
        lines.append("📅 今日无新股/新债申购。\n")

    # ==============================
    # 💰 第二部分：国债逆回购 (新增)
    # ==============================
    if repo_list:
        # 只有当利率大于 2.0 或者 是周四的时候才显示，避免垃圾时间占版面
        # 或者你可以选择永远显示
        show_repo = any(item['rate'] > 2.0 for item in repo_list) or (datetime.datetime.now().weekday() == 3)

        if show_repo:
            lines.append("💰 【闲钱理财 · 国债逆回购】")
            lines.append("💡 操作：选择【卖出】(借钱给别人)")
            lines.append("-" * 30)

            for item in repo_list:
                lines.append(f"👉 {item['name']} ({item['code']})")
                lines.append(f"   年化利率: {item['rate']}% {item['tag']}")
                lines.append(f"   每10w收益: 约 {item['profit_txt']}")
                lines.append(f"   📝 {item['advice']}")
                lines.append("-" * 30)
            lines.append("\n")
    # ==============================
    # 🚀 第一部分：LOF 套利机会
    # ==============================
    if lof_opps:
        lines.append("🚀 【LOF 高价值套利机会】")
        lines.append(f"💡 扣费标准: {COST_RATE}% | 务必试单限购")
        lines.append("-" * 30)

        for item in lof_opps:
            lines.append(f"👉 {item['name']} ({item['code']}) {item['tag']}")
            lines.append(f"   现价: {item['price']} | 溢价率: {item['premium']}%")
            lines.append(f"   💰 净利(扣费): {item['net_prem']}%")
            lines.append(f"   📝 建议: {item['advice']}")
            lines.append("-" * 30)
        lines.append("\n")
    else:
        lines.append("😴 今日无符合策略的高溢价 LOF 机会。\n")

    # ==============================
    # 📊 第二部分：LOF 市场 Top 10
    # ==============================
    lines.append("📊 【LOF 溢价率 Top 10】")

    if not lof_df.empty:
        # 准备 Top 10 数据
        top10 = lof_df.sort_values(by='premium_rate', ascending=False).head(10).copy()

        # 格式化数据以便展示
        table_data = []
        for _, row in top10.iterrows():
            name_short = row['name'][:6]  # 名字太长截断一下，防止手机换行
            vol_wan = int(row['volume'] / 10000)
            table_data.append([
                row['symbol'],
                name_short,
                f"{row['price']}",
                f"{row['premium_rate']:.2f}%",
                f"{vol_wan}万"
            ])

        # 生成 LOF 表格
        table_str = tabulate(
            table_data,
            headers=['代码', '名称', '现价', '溢价', '成交'],
            tablefmt='simple',
            stralign='right'
        )
        lines.append(table_str)
    else:
        lines.append("暂无 LOF 数据。")

    # ==============================
    # 🐢 第三部分：可转债双低策略 (新增)
    # ==============================
    if cb_opps:
        lines.append("\n" + "=" * 30)
        lines.append("🐢 【可转债 · 双低策略 Top 5】")
        lines.append("💡 逻辑: 价格+溢价率 (越低越安全)")
        lines.append("-" * 30)

        # 准备转债表格数据
        cb_table_data = []
        for item in cb_opps:
            cb_table_data.append([
                item['name'],
                f"{item['price']}",
                f"{item['premium']:.2f}%",
                f"{item['double_low']:.2f}"
            ])

        # 生成转债表格
        cb_str = tabulate(
            cb_table_data,
            headers=['名称', '价格', '溢价率', '双低值'],
            tablefmt='simple',
            stralign='right'
        )
        lines.append(cb_str)
        # --- 专门列出有新闻的转债 ---
        has_news = False
        for item in cb_opps:
            if item.get('news'):
                if not has_news:
                    lines.append("\n📰 【近期重要公告】")
                    has_news = True
                lines.append(f"• {item['name']}: {item['news']}")
        lines.append("\n📝 说明：双低值通常 <130 较安全，适合摊大饼持有。")

    # ==============================
    # ⚠️ 底部风险提示
    # ==============================
    lines.append("\n⚠️ 风险提示：")
    lines.append("1. QDII/商品LOF数据有滞后，操作前请参考期货走势。")
    lines.append("2. 转债请避免买入高价妖债，注意强赎风险。")

    return "\n".join(lines)