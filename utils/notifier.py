import json

import requests


def send_wecom_webhook(webhook_url, title, content):
    """
    发送企业微信 Webhook 通知 (纯文本模式)
    兼容性最好，支持在个人微信中查看
    """
    if not webhook_url:
        print("❌ 未配置 Webhook URL，跳过发送")
        return

    headers = {'Content-Type': 'application/json'}

    # 构造纯文本内容
    # 简单拼接标题和正文
    final_content = f"【{title}】\n\n{content}"

    data = {
        "msgtype": "text",  # <--- 改为 text
        "text": {
            "content": final_content
        }
    }

    try:
        resp = requests.post(webhook_url, headers=headers, data=json.dumps(data))

        if resp.status_code == 200:
            res_json = resp.json()
            if res_json.get('errcode') == 0:
                print("✅ 企业微信推送成功")
            else:
                print(f"❌ 推送失败，API返回: {res_json}")
        else:
            print(f"❌ 推送网络请求失败: {resp.status_code}")

    except Exception as e:
        print(f"❌ 推送过程发生错误: {e}")