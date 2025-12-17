import pandas as pd
import re
import requests
import uuid
import os
import traceback
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from io import BytesIO

app = FastAPI()

# 挂载静态目录
os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def read_root():
    return {"message": "Service is running! All systems go."}

# =================================================================
# 公共函数：自动识别列名 (Fuzzy Column Mapping)
# =================================================================
def find_columns_by_keywords(df_columns):
    """
    输入 DataFrame 的列名列表，返回一个字典，映射标准字段名到实际列名。
    """
    # 定义关键词映射表
    column_keywords = {
        'target_book_name': ['教材', '书名', '名称', '课本'],
        'target_publisher': ['出版', '版社'],
        'target_isbn': ['书号', 'ISBN', 'isbn', '标准号'],
        'target_class': ['班级', '使用班级', '适用对象', '范围']
    }
    
    found_cols = {}
    for col in df_columns:
        col_str = str(col).strip()
        for key, keywords in column_keywords.items():
            # 只要包含关键词，且该字段还没找到，就记录下来
            if key not in found_cols and any(kw in col_str for kw in keywords):
                found_cols[key] = col
    
    return found_cols

# ==========================================
# 🚪 第一扇门：处理【书单】格式 (process_excel)
# 场景：处理复杂班级名 (如: 24级护理1班)
# ==========================================
@app.post("/process")
async def process_excel(request: Request):
    data = await request.json()
    file_url = data.get('file_url')
    
    if not file_url:
        return {"error": "No file_url provided"}
    
    try:
        response = requests.get(file_url)
        response.raise_for_status()
        file_content = BytesIO(response.content)
        
        # 1. 读取 Excel
        df = pd.read_excel(file_content, sheet_name='Sheet1')
        
        # 2. 自动识别列名
        found_cols = find_columns_by_keywords(df.columns)
        
        # 检查核心列是否存在
        if 'target_class' not in found_cols or 'target_book_name' not in found_cols:
             return {"error": f"无法识别表头，请确保包含'教材名称'和'使用班级'相关列。识别结果: {list(df.columns)}"}

        # 3. 定义该接口特有的解析逻辑 (针对复杂班级名)
        def parse_class_info(class_str):
            classes = []
            # 匹配 "24护理1班（45人）"
            pattern = r'(\d{2}[^（\s]+?)（(\d+)人?）'
            matches = re.findall(pattern, str(class_str))
            for match in matches:
                classes.append((match[0], int(match[1])))
            
            # 匹配 "24护理1班（45）" - 补充匹配
            pattern2 = r'(\d{2}[^（\s]+?)（(\d+)）'
            matches2 = re.findall(pattern2, str(class_str))
            for match in matches2:
                if not any(c[0] == match[0] for c in classes):
                    classes.append((match[0], int(match[1])))
            return classes
        
        processed_data = []
        for index, row in df.iterrows():
            # 获取数据
            textbook_name = row[found_cols['target_book_name']]
            class_str = row[found_cols['target_class']]
            publisher = row[found_cols['target_publisher']] if 'target_publisher' in found_cols else ""
            isbn = row[found_cols['target_isbn']] if 'target_isbn' in found_cols else ""

            classes = parse_class_info(str(class_str))
            for class_name, student_count in classes:
                processed_data.append({
                    '原始班级': class_name,
                    '书号': isbn,
                    '书名': textbook_name,
                    '出版社': publisher,
                    '学生数量': student_count
                })
        
        result_df = pd.DataFrame(processed_data)
        if result_df.empty:
            return {"error": "No valid data extracted"}

        # 4. 班级名标准化 (该接口特有逻辑)
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

        result_df['班级'] = result_df['原始班级'].apply(normalize_class_name_final)
        
        # 5. 去重与排序
        result_df_unique = result_df.drop_duplicates(subset=['班级', '书名', '出版社', '书号']).copy()
        
        # 提取年份和专业进行排序
        result_df_unique['年份'] = result_df_unique['班级'].str[:2]
        result_df_unique['专业班级'] = result_df_unique['班级'].str[2:]
        result_df_sorted = result_df_unique.sort_values(['年份', '专业班级'], ascending=[False, True])
        
        # 生成序号
        result_df_sorted['序号'] = range(1, len(result_df_sorted) + 1)
        
        # 6. 按指定顺序输出
        final_cols = ['序号', '班级', '书号', '书名', '出版社', '学生数量']
        
        # 补齐可能缺失的列
        for col in final_cols:
            if col not in result_df_sorted.columns:
                result_df_sorted[col] = ""
                
        final_df = result_df_sorted[final_cols]
        
        # 7. 保存
        filename = f"result_{uuid.uuid4()}.xlsx"
        save_path = os.path.join("static", filename)
        final_df.to_excel(save_path, index=False)
        
        base_url = str(request.base_url).rstrip("/")
        download_url = f"{base_url}/static/{filename}"
        if download_url.startswith("http://"):
            download_url = download_url.replace("http://", "https://", 1)
        
        return {"download_url": download_url, "message": "success"}
    
    except Exception as e:
        traceback.print_exc()
        return {"error": str(e)}


# ==========================================
# 🚪 第二扇门：处理【寒假作业】格式 (process_winter_homework)
# 场景：处理简单数字班级名 (如: 101班)
# ==========================================
@app.post("/process_winter_homework")
async def process_winter_homework(request: Request):
    data = await request.json()
    file_url = data.get('file_url')
    if not file_url:
        return {"error": "请提供文件链接"}

    try:
        response = requests.get(file_url)
        file_content = BytesIO(response.content)
        
        # 1. 读取 Excel
        df = pd.read_excel(file_content, sheet_name='Sheet1')

        # 2. 自动识别列名 (使用公共逻辑)
        found_cols = find_columns_by_keywords(df.columns)

        if 'target_class' not in found_cols or 'target_book_name' not in found_cols:
            return {"error": f"无法识别表头，请确保包含'教材名称'和'使用班级'相关列。识别结果: {list(df.columns)}"}

        # 3. 定义该接口特有的解析逻辑 (针对 "101班 40人" 这种格式)
        def parse_class_info_new(class_str):
            classes = []
            s = str(class_str)
            # 模式1: "101班 45人"
            pattern = r'(\d+班)\s*(\d+)人'
            matches = re.findall(pattern, s)
            for match in matches:
                classes.append((match[0], int(match[1])))
            
            if not classes:
                # 模式2: "101班 45" (没有'人'字)
                pattern2 = r'(\d+班)\s*(\d+)'
                matches2 = re.findall(pattern2, s)
                for match in matches2:
                    classes.append((match[0], int(match[1])))
            return classes

        processed_data = []
        
        for index, row in df.iterrows():
            textbook_name = row[found_cols['target_book_name']]
            class_str = row[found_cols['target_class']]
            publisher = row[found_cols['target_publisher']] if 'target_publisher' in found_cols else ""
            isbn = row[found_cols['target_isbn']] if 'target_isbn' in found_cols else ""

            if pd.isna(class_str) or str(class_str).strip() == '':
                continue
            
            classes = parse_class_info_new(class_str)
            for class_name, student_count in classes:
                processed_data.append({
                    '班级': class_name,
                    '书号': isbn,
                    '书名': textbook_name,
                    '出版社': publisher,
                    '学生数量': student_count
                })

        result_df = pd.DataFrame(processed_data)
        if result_df.empty:
            return {"error": "未能解析出有效数据，请检查班级列格式"}

        # 4. 排序逻辑 (针对数字班级排序)
        # 提取数字进行排序: 101班 -> 101
        result_df['班级编号数字'] = result_df['班级'].astype(str).str.replace('班', '', regex=False)
        result_df = result_df[result_df['班级编号数字'].str.isnumeric()] 
        result_df['班级编号数字'] = result_df['班级编号数字'].astype(int)
        
        result_df_sorted = result_df.sort_values('班级编号数字', ascending=True)

        # 5. 去重
        result_df_unique = result_df_sorted.drop_duplicates(subset=['班级', '书名', '出版社', '书号']).copy()

        # 生成序号
        result_df_unique['序号'] = range(1, len(result_df_unique) + 1)

        # 6. 按指定顺序输出
        final_cols = ['序号', '班级', '书号', '书名', '出版社', '学生数量']
        
        for col in final_cols:
            if col not in result_df_unique.columns:
                result_df_unique[col] = ""

        final_df = result_df_unique[final_cols].reset_index(drop=True)

        # 7. 保存与返回
        filename = f"winter_hw_{uuid.uuid4()}.xlsx"
        save_path = os.path.join("static", filename)
        final_df.to_excel(save_path, index=False)

        base_url = str(request.base_url).rstrip("/")
        download_url = f"{base_url}/static/{filename}"
        if download_url.startswith("http://"):
            download_url = download_url.replace("http://", "https://", 1)

        return {"download_url": download_url, "message": "寒假作业处理完成"}

    except Exception as e:
        traceback.print_exc()
        return {"error": f"处理出错: {str(e)}"}
