import os
import pandas as pd
import pymssql
from dotenv import load_dotenv
import re
import warnings
import requests
import json
import time
import random

# 过滤警告
warnings.filterwarnings('ignore', category=UserWarning, message='pandas only supports SQLAlchemy connectable')

load_dotenv()

# 数据库配置
mysql_ip = os.getenv('sqlserver_ip')
mysql_port = os.getenv('sqlserver_port')
mysql_user_name = os.getenv('sqlserver_user_name')
mysql_user_pwd = os.getenv('sqlserver_user_pwd')
mysql_order_db = os.getenv('sqlserver_db')

# 硅基流动API配置 - 使用正确的参数
SILICONFLOW_API_KEY = ''
SILICONFLOW_API_URL = ""
MODEL_NAME = "Qwen/Qwen3-8B"


def fix_encoding(text):
    """修复编码问题"""
    if not isinstance(text, str):
        return text

    encodings_to_try = [
        ('latin-1', 'utf-8'),
        ('latin-1', 'gbk'),
        ('latin-1', 'gb2312'),
        ('utf-8', 'utf-8'),
    ]

    for src_enc, dst_enc in encodings_to_try:
        try:
            return text.encode(src_enc).decode(dst_enc)
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
    return text


def split_product_info(text):
    """根据丨和汉字拆分产品信息"""
    if not isinstance(text, str) or not text.strip():
        return {'brand': '', 'english_name': '', 'chinese_name': '', 'full_text': text}

    cleaned_text = fix_encoding(text)
    pattern = r'^([^丨]+)丨([^\u4e00-\u9fa5]*)([\u4e00-\u9fa5].*)$'

    match = re.match(pattern, cleaned_text)
    if match:
        brand = match.group(1).strip()
        english_part = match.group(2).strip()
        chinese_part = match.group(3).strip()

        return {
            'brand': brand,
            'english_name': english_part,
            'chinese_name': chinese_part,
            'full_text': cleaned_text
        }
    else:
        chinese_chars = re.findall(r'[\u4e00-\u9fa5]', cleaned_text)
        if chinese_chars:
            first_chinese_index = cleaned_text.find(chinese_chars[0])
            return {
                'brand': '',
                'english_name': cleaned_text[:first_chinese_index].strip(),
                'chinese_name': cleaned_text[first_chinese_index:].strip(),
                'full_text': cleaned_text
            }
        else:
            return {
                'brand': '',
                'english_name': cleaned_text,
                'chinese_name': '',
                'full_text': cleaned_text
            }


def generate_ai_review(product_info, max_retries=2):
    """
    使用硅基流动API生成商品评论
    """
    brand = product_info.get('brand', '')
    english_name = product_info.get('english_name', '')
    chinese_name = product_info.get('chinese_name', '')

    # 构建提示词
    prompt = f"为{brand}品牌的{chinese_name}写一条50字左右的真实用户评论，语气自然，包含具体使用感受。"

    headers = {
        "Authorization": f"Bearer {SILICONFLOW_API_KEY}",
        "Content-Type": "application/json"
    }

    # 使用正确的payload格式（基于curl命令）
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "stream": False,
        "max_tokens": 200,
        "temperature": 0.7,
        "top_p": 0.7,
        "top_k": 50,
        "frequency_penalty": 0.5,
        "n": 1,
        "response_format": {
            "type": "text"
        }
    }

    for attempt in range(max_retries):
        try:
            print(f"  发送API请求... (尝试 {attempt + 1})")
            response = requests.post(SILICONFLOW_API_URL, headers=headers,
                                     json=payload, timeout=30)

            print(f"  响应状态码: {response.status_code}")

            if response.status_code == 200:
                result = response.json()
                print(f"  API响应: {result}")

                if 'choices' in result and len(result['choices']) > 0:
                    review = result['choices'][0]['message']['content'].strip()
                    print(f"  生成评论成功: {review[:50]}...")
                    return review
                else:
                    print("  API返回格式异常")
            else:
                error_msg = response.text
                print(f"  API错误: {response.status_code} - {error_msg}")

        except json.JSONDecodeError as e:
            print(f"  JSON解析失败: {e}")
            print(f"  响应内容: {response.text[:200] if 'response' in locals() else '无响应'}")
        except Exception as e:
            print(f"  请求异常: {e}")

        if attempt < max_retries - 1:
            time.sleep(2)

    # 如果API调用失败，返回模拟评论
    print("  API调用失败，使用本地生成评论")
    return generate_smart_review(product_info)


def generate_smart_review(product_info):
    """
    智能本地生成评论，不依赖API
    """
    brand = product_info.get('brand', 'lululemon')
    chinese_name = product_info.get('chinese_name', '')

    # 根据产品特征生成相关评论
    features = {
        '水瓶': ['容量刚好', '密封性很好不会漏水', '携带方便', '材质安全没有异味', '设计人性化'],
        '紧身裤': ['弹性很好不勒肉', '透气性不错', '支撑性很好', '面料柔软亲肤', '运动时很舒适'],
        '背包': ['容量很大', '分层设计合理', '背起来很舒适', '防水效果不错', '设计时尚'],
        '短裤': ['穿着舒适', '透气性好', '设计合理', '运动时很方便', '材质不错'],
        '卫衣': ['保暖性好', '面料柔软', '设计时尚', '做工精细', '穿着舒适'],
        '内裤': ['贴身舒适', '透气性好', '不易变形', '设计合理', '材质亲肤'],
        '短裙': ['设计好看', '穿着舒适', '材质不错', '版型很好', '搭配方便']
    }

    # 匹配产品类型
    product_type = None
    for key in features.keys():
        if key in chinese_name:
            product_type = key
            break

    # 80%好评，20%差评
    is_positive = random.random() > 0.2

    if product_type:
        feature = random.choice(features[product_type])
        if is_positive:
            reviews = [
                f"这款{chinese_name}真的很不错，{feature}，使用体验很棒，{brand}的产品质量值得信赖。",
                f"买了{chinese_name}后很满意，{feature}，设计也很人性化，会推荐给朋友。",
                f"{brand}的{chinese_name}超出了我的预期，{feature}，细节处理到位，性价比很高。",
                f"穿着{chinese_name}运动很舒适，{feature}，设计合理，非常满意这次购物。"
            ]
        else:
            reviews = [
                f"{chinese_name}的{feature.split('很')[0]}方面有待改进，和预期有些差距，希望{brand}能优化。",
                f"产品整体还可以，但{feature.split('很')[0]}不如描述的那么好，建议改进细节。",
                f"对{chinese_name}有些失望，{feature.split('很')[0]}表现一般，希望质量能更稳定。"
            ]
    else:
        # 通用评论
        if is_positive:
            reviews = [
                f"{brand}的产品质量一直很稳定，这款{chinese_name}也没有让我失望，细节处理得很好。",
                f"使用了一段时间的{chinese_name}，体验很不错，设计合理，材质舒适，值得推荐。",
                f"收到{chinese_name}后马上试用了，效果很好，做工精细，这个价格很值。"
            ]
        else:
            reviews = [
                f"对这款{chinese_name}总体满意，有些小细节可以改进，但整体性价比还是不错的。",
                f"{chinese_name}的质量一般，有些地方做工不够精细，希望能改进。"
            ]

    return random.choice(reviews)


def main():
    try:
        # 连接数据库
        conn = pymssql.connect(
            server=mysql_ip,
            user=mysql_user_name,
            password=mysql_user_pwd,
            database=mysql_order_db,
            port=int(mysql_port) if mysql_port else 1433,
            charset='UTF-8'
        )

        # SQL查询 - 先测试3条
        query_sql = """
        SELECT TOP 3 
            order_id,
            user_id,
            product_id,
            total_amount,
            product_name
        FROM oms_order_dtl;
        """

        # 获取数据
        df = pd.read_sql_query(query_sql, conn)
        print(f"获取到 {len(df)} 条数据")

        # 处理产品名称拆分
        results = []
        for product_name in df['product_name']:
            result = split_product_info(product_name)
            results.append(result)

        # 合并结果
        result_df = pd.DataFrame(results)
        final_df = pd.concat([df, result_df], axis=1)

        # 为每个产品生成评论
        print("开始生成AI评论...")
        reviews = []

        for i, row in final_df.iterrows():
            print(f"\n正在生成第 {i + 1}/{len(final_df)} 条评论...")
            print(f"  产品: {row['brand']} - {row['chinese_name']}")

            product_info = {
                'brand': row['brand'],
                'english_name': row['english_name'],
                'chinese_name': row['chinese_name']
            }

            review = generate_ai_review(product_info)
            reviews.append(review)

            # 添加延迟避免API限制
            time.sleep(3)

        # 添加评论列
        final_df['ai_review'] = reviews

        # 显示结果
        print("\n" + "=" * 100)
        print("产品信息及AI评论结果:")
        print("=" * 100)
        for i, row in final_df.iterrows():
            print(f"\n产品 {i + 1}:")
            print(f"  品牌: {row['brand']}")
            print(f"  英文名: {row['english_name']}")
            print(f"  中文描述: {row['chinese_name']}")
            print(f"  AI评论: {row['ai_review']}")

        # 保存结果
        # output_file = "products_with_reviews.csv"
        # final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        # print(f"\n结果已保存到: {output_file}")

        conn.close()

    except Exception as e:
        print(f"数据处理失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":

    main()
