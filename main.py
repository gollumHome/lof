from config import TARGET_LOFS, MIN_VOLUME, THRESHOLD_QDII, THRESHOLD_LOCAL, WECOM_WEBHOOK_URL
from utils.data_fetcher import fetch_lof_data, fetch_cb_data, fetch_today_ipo, fetch_repo_data
from utils.strategy import analyze_single_lof, filter_double_low_cb, analyze_repo_strategy
from utils.formatter import format_text_report
from utils.notifier import send_wecom_webhook


def filter_opportunities(df):
    """
    根据白名单和阈值筛选机会
    """
    opps = []

    # 遍历白名单配置
    for code, lof_type in TARGET_LOFS.items():
        # 在大表中找数据
        matches = df[df['symbol'] == code]
        if matches.empty: continue

        row = matches.iloc[0]

        # 基础过滤
        if row['volume'] < MIN_VOLUME: continue

        threshold = THRESHOLD_QDII if lof_type == 'QDII' else THRESHOLD_LOCAL

        if row['premium_rate'] > threshold:
            # 调用策略分析
            analysis = analyze_single_lof(row)

            opps.append({
                "code": code,
                "name": row['name'],
                "price": row['price'],
                "premium": round(row['premium_rate'], 2),
                "volume": int(row['volume']),
                "tag": analysis['risk_tag'],
                "net_prem": analysis['net_premium'],
                "advice": analysis['advice']
            })

    # 按溢价率排序
    opps.sort(key=lambda x: x['premium'], reverse=True)
    return opps


if __name__ == "__main__":
    print(">>> 启动 A股全能挖掘机 <<<")

    # 1. [新增] 获取今日打新数据
    ipo_data = fetch_today_ipo()

    # 2.国债逆回购
    repo_df = fetch_repo_data()
    repo_opps = []
    if not repo_df.empty:
        repo_opps = analyze_repo_strategy(repo_df)

    # 3. 获取 LOF 数据
    lof_df = fetch_lof_data()
    lof_opps = []
    if not lof_df.empty:
        # 使用全市场扫描模式 (我们在上一步讨论过的优化)
        lof_opps = filter_opportunities(lof_df)

    # 4. 获取 可转债 数据
    cb_df = fetch_cb_data()
    cb_opps = []
    if not cb_df.empty:
        cb_opps = filter_double_low_cb(cb_df, limit=5)

    # 5. 生成综合报告
    # 只要有任意一种机会，就发送推送
    has_opportunity = (
            (ipo_data['stocks'] or ipo_data['bonds']) or
            repo_opps or
            lof_opps or
            cb_opps
    )

    if has_opportunity:
        # 注意参数顺序要对应 formatter 的定义
        report_text = format_text_report(lof_df, lof_opps, cb_opps, ipo_data,repo_opps)

        print(report_text)  # 本地预览
        send_wecom_webhook(WECOM_WEBHOOK_URL, "A股投资日报", report_text)
    else:
        print("今日全市场静悄悄，无任何机会。")