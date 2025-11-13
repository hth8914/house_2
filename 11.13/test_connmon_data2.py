#!/usr/bin/python
# coding:utf-8
"""
工单编号：大数据-用户画像-11-达摩盘基础特征
修复版：优化API请求，减少超时
"""
import os
import re
import pandas as pd
import requests
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, NVARCHAR
import time
import random

# 方优

load_dotenv()
mssql_connection = f"mssql+pymssql://{os.getenv('sqlserver_user_name')}:{os.getenv('sqlserver_user_pwd')}@{os.getenv('sqlserver_ip')}:{os.getenv('sqlserver_port')}/{os.getenv('sqlserver_db')}?charset=utf8"
engine = create_engine(mssql_connection)

# 1. 拉订单
print(">>> 开始拉取订单数据...")
df = pd.read_sql_query("""
SELECT order_id, user_id, product_id, total_amount, product_name, ds, ts
FROM realtime_v3.dbo.oms_order_dtl
""", engine)
print(f">>> 订单数据拉取完成，共 {len(df)} 条记录")

# 2. 商品分类
def extract_class(name: str) -> str:
    try:
        if "丨" in str(name):
            _, desc = name.split("丨", 1)
        else:
            desc = name
        m = re.match(r"([A-Za-z0-9\-_.™]+(?:\s[A-Za-z0-9\-_.™]+)?)", str(desc).strip())
        return m.group(1) if m else "其他"
    except:
        return "其他"

print(">>> 开始商品分类...")
df["product_class"] = df["product_name"].apply(extract_class)

# 3. 加载敏感词文件
def load_sensitive_words(file_path):
    """加载敏感词文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            words = [line.strip() for line in f if line.strip()]
        print(f">>> 加载敏感词: {len(words)} 个")
        return words
    except Exception as e:
        print(f">>> 加载敏感词失败: {e}")
        return ["垃圾", "坑人", "骗钱", "劣质", "假货", "破烂", "废物", "黑心", "太差", "糟糕"]

sensitive_words = load_sensitive_words(r"/SqlServer_sqlserver/suspected-sensitive-words.txt")

# 4. 评论生成
# API_KEY = "sk-oaobdjsmuqydjerozbifhjydvddshdfosqltitlxroymmryc"
# URL = "https://api.siliconflow.cn/v1/chat/completions"
# MODEL = "Qwen/Qwen2.5-7B-Instruct"

# 修改后的
API_KEY = 'sk-ejamrrtosuwvmaepedfknfkbazxgrkfjcwjwjszatalfmllz'
URL = "https://api.siliconflow.cn/v1/chat/completions"
MODEL = "Qwen/Qwen2.5-7B-Instruct"

def test_api_connection():
    """测试API连接"""
    print(">>> 测试API连接...")
    test_payload = {
        "model": MODEL,
        "messages": [{"role": "user", "content": "请用中文回复'连接成功'"}],
        "max_tokens": 10,
        "temperature": 0.1
    }
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json; charset=utf-8"
    }

    try:
        resp = requests.post(URL, headers=headers, json=test_payload, timeout=10)
        print(f">>> API测试状态码: {resp.status_code}")
        if resp.status_code == 200:
            result = resp.json()["choices"][0]["message"]["content"]
            print(f">>> API连接正常: {result}")
            return True
        else:
            print(f">>> API异常: {resp.text[:100]}")
            return False
    except Exception as e:
        print(f">>> API连接失败: {e}")
        return False

def safe_gen_comment(name: str) -> str:
    """安全生成评论，控制好评差评比例"""
    # 控制好评率60%，差评率40%
    is_positive = random.random() < 0.6

    if is_positive:
        # 生成好评 - 简化提示词
        prompt = f"写一条商品正面评价：{name}（30字内）"
    else:
        # 生成差评 - 简化提示词
        prompt = f"写一条商品负面评价：{name}（30字内）"

    payload = {
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7,  # 降低随机性
        "max_tokens": 40,    # 减少输出长度
        "stream": False      # 关闭流式输出
    }
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json; charset=utf-8"
    }

    try:
        # 使用更短的超时时间
        resp = requests.post(URL, headers=headers, json=payload, timeout=10)
        if resp.status_code == 200:
            content = resp.json()["choices"][0]["message"]["content"].strip()
            # 清理内容，去除"好评"、"差评"字样
            content = re.sub(r'(好评|差评)[：:]?\s*', '', content)
            content = re.sub(r'[^\u4e00-\u9fa5，。！？；："”‘\'（）【】、\s\w]', '', content)

            # 如果是差评但攻击性不够，添加敏感词
            if not is_positive and not any(word in content for word in sensitive_words):
                if random.random() < 0.7:  # 70%概率增强攻击性
                    content = content + " " + random.choice(sensitive_words) + "！"

            return content if content else ("质量很好，满意" if is_positive else "质量很差，失望")
        else:
            print(f"  API返回错误: {resp.status_code}")
            return "评论生成失败"
    except requests.Timeout:
        print("  请求超时")
        return "生成超时"
    except Exception as e:
        print(f"  请求异常: {e}")
        return "生成异常"

# 优化后的备用评论池（不带好评差评标识）
positive_templates = [
    "质量很好，穿着舒适，会继续购买",
    "包装精美，发货迅速，很满意",
    "物超所值，强烈推荐给大家",
    "穿着舒服，弹性很好，运动无束缚",
    "面料柔软，透气性佳，夏天穿很凉快",
    "版型好看，显瘦效果不错",
    "做工精细，细节处理得很好",
    "颜色正，和图片一模一样",
    "穿着合身，尺码标准，很满意",
    "性价比高，质量对得起价格"
]

negative_templates = [
    "质量太差了，穿一次就开线",
    "尺寸完全不对，根本穿不了",
    "面料粗糙，穿着很不舒服",
    "色差严重，和图片完全不一样",
    "做工粗糙，线头到处都是",
    "价格虚高，根本不值这个价",
    "快递太慢，等了好久才收到",
    "客服态度差，问题解决不了",
    "材质很差，洗一次就变形",
    "设计不合理，穿着很难受"
]

def get_fallback_comment(name):
    """获取备用评论，按比例分配"""
    if random.random() < 0.6:  # 60%好评
        comment = random.choice(positive_templates)
    else:  # 40%差评
        comment = random.choice(negative_templates)
        # 70%概率为差评添加敏感词增强攻击性
        if random.random() < 0.7:
            comment = comment + " " + random.choice(sensitive_words) + "！"
    return comment

print(">>> 开始生成商品评论...")

if not test_api_connection():
    print(">>> API连接失败，使用备用评论方案")
    comments = [get_fallback_comment(name) for name in df["product_name"]]
else:
    print(">>> API连接成功，开始生成真实评论")
    comments = []
    total_rows = len(df)
    positive_count = 0
    negative_count = 0
    api_success_count = 0
    api_fail_count = 0

    for idx, name in enumerate(df["product_name"]):
        if idx % 10 == 0 or idx == total_rows - 1:  # 更频繁的进度显示
            print(f">>> 进度: {idx+1}/{total_rows} ({(idx+1)/total_rows*100:.1f}%)")

        comment = safe_gen_comment(name)

        # 统计API成功率
        if comment in ["评论生成失败", "生成超时", "生成异常"]:
            api_fail_count += 1
            comment = get_fallback_comment(name)
            print(f"  第{idx+1}条使用备用评论 (失败累计: {api_fail_count})")
        else:
            api_success_count += 1

        # 统计好评差评数量
        if any(word in comment for word in ["很好", "满意", "推荐", "舒服", "不错", "喜欢", "舒适", "精美", "超值"]):
            positive_count += 1
        elif any(word in comment for word in sensitive_words + ["差", "不好", "失望", "垃圾", "坑", "劣质", "粗糙"]):
            negative_count += 1
        else:
            # 默认归类为好评
            positive_count += 1

        comments.append(comment)
        time.sleep(0.5)  # 增加间隔，避免频率过高

    # 输出详细统计
    print(f">>> API调用统计: 成功 {api_success_count}条, 失败 {api_fail_count}条")
    total_generated = positive_count + negative_count
    if total_generated > 0:
        positive_ratio = positive_count / total_generated * 100
        negative_ratio = negative_count / total_generated * 100
        print(f">>> 评论比例: 好评 {positive_ratio:.1f}% ({positive_count}条), 差评 {negative_ratio:.1f}% ({negative_count}条)")

df["comment"] = comments

# 5. 统计评论生成情况
success_count = sum(1 for c in comments if c not in ["评论生成失败", "生成超时"] and "生成异常" not in c)
print(f">>> 评论生成统计: 成功 {success_count}/{len(comments)} 条")

# 6. 写入数据库
print(">>> 开始写入数据库...")
df_out = df[[
    "order_id", "user_id", "product_id", "total_amount",
    "product_name", "product_class", "ds", "ts", "comment"
]]

df_out.to_sql(
    "orders_portrait",
    con=engine,
    if_exists="replace",
    index=False,
    method="multi",
    chunksize=1000,
    dtype={
        "product_name": NVARCHAR(500),
        "product_class": NVARCHAR(100),
        "comment": NVARCHAR(500)
    }
)

print(f">>> 已完成！共处理 {len(df_out)} 条记录")
print(">>> 数据已写入表: orders_portrait")
