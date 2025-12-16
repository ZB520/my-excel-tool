import pandas as pd
import re
import requests
import uuid
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from io import BytesIO

app = FastAPI()

# 挂载静态目录，用于下载生成的 Excel
os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def read_root():
    return {"message": "Service is running!"}

@app.post("/process")
async def process_excel(request: Request):
    # 1. 获取扣子传来的参数
    data = await request.json()
    file_url = data.get('file_url')
    
    if not file_url:
        return {"error": "No file_url provided"}
    
    try:
        # 2. 下载文件
        response = requests.get(file_url)
        response.raise_for_status()
        file_content = BytesIO(response.content)
    
        # 3. 处理逻辑 (你的核心代码)
        df = pd.read_excel(file_content, sheet_name='Sheet1')
        
        # --- 数据清洗 ---
        new_columns = ['序号', '教材名称', '出版社', '书号', '使用班级']
        df_clean = df.copy()
        if len(df_clean.columns) >= len(new_columns):
            df_clean.columns = new_columns
        else:
            df_clean = df_clean.iloc[:, :5]
            df_clean.columns = new_columns
    
        df_clean = df_clean.drop(0).reset_index(drop=True)
    
        def parse_class_info(class_str):
            classes = []
            pattern = r'(\d{2}[^（\s]+?)（(\d+)人?）'
            matches = re.findall(pattern, str(class_str))
            for match in matches:
                classes.append((match[0], int(match[1])))
            
            pattern2 = r'(\d{2}[^（\s]+?)（(\d+)）'
            matches2 = re.findall(pattern2, str(class_str))
            for match in matches2:
                if not any(c[0] == match[0] for c in classes):
                    classes.append((match[0], int(match[1])))
            return classes
    
        processed_data = []
        for index, row in df_clean.iterrows():
            classes = parse_class_info(str(row['使用班级']))
            for class_name, student_count in classes:
                processed_data.append({
                    '教材名称': row['教材名称'],
                    '出版社': row['出版社'],
                    '书号': row['书号'],
                    '班级': class_name,
                    '人数': student_count
                })
    
        result_df = pd.DataFrame(processed_data)
        
        if result_df.empty:
            return {"error": "No valid data extracted"}
    
        # --- 这里的逻辑和你原来的一样 ---
        def normalize_class_name_final(class_name):
            if '人）' in class_name or '）' in class_name:
                match = re.search(r'(2[45][^（）\s]+)', class_name)
                if match: return match.group(1)
            if '级' in class_name and class_name.startswith(('24', '25')):
                year = class_name[:2]
                major = class_name[3:]
                if major.startswith('级'): major = major[1:]
                return year + major
            return class_name
    
        result_df['标准化班级'] = result_df['班级'].apply(normalize_class_name_final)
        result_df_unique = result_df.drop_duplicates(subset=['标准化班级', '教材名称', '出版社', '书号']).copy()
        result_df_unique['年份'] = result_df_unique['标准化班级'].str[:2]
        result_df_unique['专业班级'] = result_df_unique['标准化班级'].str[2:]
        result_df_sorted = result_df_unique.sort_values(['年份', '专业班级'], ascending=[False, True])
    
        unique_classes_sorted = result_df_sorted['标准化班级'].drop_duplicates().tolist()
        class_numbers = {name: i for i, name in enumerate(unique_classes_sorted, 1)}
        result_df_sorted['序号'] = result_df_sorted['标准化班级'].map(class_numbers)
        
        final_df = result_df_sorted[['序号', '标准化班级', '书号', '教材名称', '出版社', '人数']].copy()
        final_df = final_df.rename(columns={'标准化班级': '班级', '教材名称': '书名'})
    
        # 4. 保存文件
        filename = f"result_{uuid.uuid4()}.xlsx"
        save_path = os.path.join("static", filename)
        final_df.to_excel(save_path, index=False)
    
        # 5. 生成下载链接 (自动获取当前域名)
        base_url = str(request.base_url).rstrip("/")
        download_url = f"{base_url}/static/{filename}"
        
        # 强制 HTTPS (为了防止扣子报错)
        if download_url.startswith("http://"):
            download_url = download_url.replace("http://", "https://", 1)
    
        return {
            "download_url": download_url,
            "message": "success"
        }
    
    except Exception as e:

        return {"error": str(e)}
# ==========================================
# 🚪 第二扇门：处理【交通职高2401班38人】格式# 格
式#==========================================
async def process_winter_homework(request: Request):
    # 1. 获取文件链接
    data = await request.json()
    file_url = data.get('file_url')
    if not file_url:
        return {"error": "请提供文件链接"}

    try:
        # 2. 下载文件
        response = requests.get(file_url)
        file_content = BytesIO(response.content)

        # 3. 读取 Excel
        df = pd.read_excel(file_content, sheet_name='Sheet1')

        # === 你的核心逻辑开始 ===
        
        # 清理数据，重新设置列名
        new_columns = ['序号', '教材名称', '出版社', '书号', '使用班级']
        df_clean = df.copy()
        # 简单容错：如果列数不够，截取前5列；够的话直接改名
        if len(df_clean.columns) >= 5:
            df_clean = df_clean.iloc[:, :5]
        df_clean.columns = new_columns

        # 删除第一行（原来的列名行）
        df_clean = df_clean.drop(0).reset_index(drop=True)

        # 定义解析函数 (你的新正则逻辑)
        def parse_class_info_new(class_str):
            classes = []
            s = str(class_str) # 强制转字符串防止报错
            
            # 模式1：2401班40人
            pattern = r'(\d+班)\s*(\d+)人'
            matches = re.findall(pattern, s)
            for match in matches:
                classes.append((match[0], int(match[1])))
            
            # 模式2：如果没有匹配到，尝试 2401班40 (无"人"字)
            if not classes:
                pattern2 = r'(\d+班)\s*(\d+)'
                matches2 = re.findall(pattern2, s)
                for match in matches2:
                    classes.append((match[0], int(match[1])))
            
            return classes

        processed_data = []

        # 遍历处理
        for index, row in df_clean.iterrows():
            textbook_name = row['教材名称']
            publisher = row['出版社']
            isbn = row['书号']
            class_info = row['使用班级']
            
            # 空值跳过
            if pd.isna(class_info) or str(class_info).strip() == '':
                continue
            
            classes = parse_class_info_new(class_info)
            
            for class_name, student_count in classes:
                processed_data.append({
                    '教材名称': textbook_name,
                    '出版社': publisher,
                    '书号': isbn,
                    '班级': class_name,
                    '人数': student_count
                })

        # 生成结果 DataFrame
        result_df = pd.DataFrame(processed_data)
        
        if result_df.empty:
            return {"error": "未能解析出有效数据，请检查上传文件的格式。"}

        # === 排序与编号逻辑 ===
        
        # 提取数字用于排序 (2401班 -> 2401)
        # 先转为字符串再替换，防止非字符串类型报错
        result_df['班级编号数字'] = result_df['班级'].astype(str).str.replace('班', '', regex=False)
        # 过滤掉非数字的行（防止脏数据报错），转为int
        result_df = result_df[result_df['班级编号数字'].str.isnumeric()] 
        result_df['班级编号数字'] = result_df['班级编号数字'].astype(int)

        # 排序
        result_df_sorted = result_df.sort_values('班级编号数字', ascending=True)

        # 去重
        result_df_unique = result_df_sorted.drop_duplicates(subset=['班级', '教材名称', '出版社', '书号']).copy()

        # 生成简易编号 (1, 2, 3...)
        unique_classes_sorted = result_df_unique['班级'].drop_duplicates().tolist()
        class_numbers = {name: i for i, name in enumerate(unique_classes_sorted, 1)}
        
        result_df_unique['编号'] = result_df_unique['班级'].map(class_numbers)

        # 整理最终列顺序
        final_df = result_df_unique[['编号', '班级', '人数', '教材名称', '出版社', '书号']].reset_index(drop=True)

        # === 你的核心逻辑结束 ===

        # 4. 保存文件并生成链接
        filename = f"winter_hw_{uuid.uuid4()}.xlsx" # 这里的名字可以改
        save_path = os.path.join("static", filename)
        final_df.to_excel(save_path, index=False)

        # 获取下载链接
        base_url = str(request.base_url).rstrip("/")
        download_url = f"{base_url}/static/{filename}"
        if download_url.startswith("http://"):
            download_url = download_url.replace("http://", "https://", 1)

        return {
            "download_url": download_url,
            "message": "寒假作业处理完成"
        }

    except Exception as e:
        return {"error": f"处理出错: {str(e)}"}
