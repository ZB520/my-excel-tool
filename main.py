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
            if key not in found_cols and any(kw in col_str for kw in keywords):
                found_cols[key] = col
    
    return found_cols

# ==========================================
# 🚪 第一扇门：处理【书单】格式 (process_excel)
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
        
        df = pd.read_excel(file_content, sheet_name='Sheet1')
        found_cols = find_columns_by_keywords(df.columns)
        
        if 'target_class' not in found_cols or 'target_book_name' not in found_cols:
             return {"error": f"无法识别表头，请确保包含'教材名称'和'使用班级'相关列。识别结果: {list(df.columns)}"}

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
        for index, row in df.iterrows():
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
        
        result_df_unique = result_df.drop_duplicates(subset=['班级', '书名', '出版社', '书号']).copy()
        
        result_df_unique['年份'] = result_df_unique['班级'].str[:2]
        result_df_unique['专业班级'] = result_df_unique['班级'].str[2:]
        result_df_sorted = result_df_unique.sort_values(['年份', '专业班级'], ascending=[False, True])
        
        # ==================== 修复逻辑 START ====================
        # 获取唯一的班级列表（保持排序顺序）
        unique_classes = result_df_sorted['班级'].drop_duplicates().tolist()
        # 创建映射字典：{ '24护理1班': 1, '24护理2班': 2, ... }
        class_map = {name: i for i, name in enumerate(unique_classes, 1)}
        # 映射序号
        result_df_sorted['序号'] = result_df_sorted['班级'].map(class_map)
        # ==================== 修复逻辑 END ======================
        
        final_cols = ['序号', '班级', '书号', '书名', '出版社', '学生数量']
        for col in final_cols:
            if col not in result_df_sorted.columns:
                result_df_sorted[col] = ""
                
        final_df = result_df_sorted[final_cols]
        
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
        
        df = pd.read_excel(file_content, sheet_name='Sheet1')
        found_cols = find_columns_by_keywords(df.columns)

        if 'target_class' not in found_cols or 'target_book_name' not in found_cols:
            return {"error": f"无法识别表头，请确保包含'教材名称'和'使用班级'相关列。识别结果: {list(df.columns)}"}

        def parse_class_info_new(class_str):
            classes = []
            s = str(class_str)
            pattern = r'(\d+班)\s*(\d+)人'
            matches = re.findall(pattern, s)
            for match in matches:
                classes.append((match[0], int(match[1])))
            
            if not classes:
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

        result_df['班级编号数字'] = result_df['班级'].astype(str).str.replace('班', '', regex=False)
        result_df = result_df[result_df['班级编号数字'].str.isnumeric()] 
        result_df['班级编号数字'] = result_df['班级编号数字'].astype(int)
        
        result_df_sorted = result_df.sort_values('班级编号数字', ascending=True)
        result_df_unique = result_df_sorted.drop_duplicates(subset=['班级', '书名', '出版社', '书号']).copy()

        # ==================== 修复逻辑 START ====================
        # 1. 提取所有不重复的班级，保持排序顺序
        unique_classes = result_df_unique['班级'].drop_duplicates().tolist()
        # 2. 生成班级ID字典：{'101班': 1, '102班': 2, ...}
        class_map = {name: i for i, name in enumerate(unique_classes, 1)}
        # 3. 将ID映射回数据框
        result_df_unique['序号'] = result_df_unique['班级'].map(class_map)
        # ==================== 修复逻辑 END ======================

        final_cols = ['序号', '班级', '书号', '书名', '出版社', '学生数量']
        for col in final_cols:
            if col not in result_df_unique.columns:
                result_df_unique[col] = ""

        final_df = result_df_unique[final_cols].reset_index(drop=True)

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

# ==========================================
# 🚪 第三扇门：处理【西湖职高】格式 (最终版)
# ==========================================
@app.post("/process_westlake")
async def process_westlake(request: Request):
    # 1. 获取文件链接
    data = await request.json()
    file_url = data.get('file_url')
    if not file_url:
        return {"error": "请提供文件链接"}

    try:
        # 2. 下载文件
        response = requests.get(file_url)
        response.raise_for_status()
        file_content = BytesIO(response.content)

        # 3. 读取 Excel
        df = pd.read_excel(file_content, sheet_name='Sheet1')

        # === 核心处理逻辑 (您的最终版代码) ===

        # 清理数据，重新设置列名
        new_columns = ['序号', '教材名称', '出版社', '书号', '使用班级']
        df_clean = df.copy()
        
        # 容错：确保列数足够
        if len(df_clean.columns) >= 5:
            df_clean = df_clean.iloc[:, :5]
        df_clean.columns = new_columns

        # 删除第一行
        df_clean = df_clean.drop(0).reset_index(drop=True)

        # 定义解析函数 (基于您的最终版逻辑)
        def parse_class_info_new_format(class_str):
            classes = []
            s = str(class_str)
            
            # 先清理字符串，去掉括号和括号里的内容
            cleaned_str = re.sub(r'（[^）]*）', '', s)  # 去掉中文括号内容
            cleaned_str = re.sub(r'\([^)]*\)', '', cleaned_str)  # 去掉英文括号内容
            cleaned_str = cleaned_str.strip(' 、，,')  # 去掉首尾的分隔符
            
            # 格式1：茶艺231-45 → 23茶艺1 (专业+年份班级-人数)
            pattern1 = r'([\u4e00-\u9fa5]+)(\d{2})(\d+)(?:-(\d+))?'
            matches1 = re.findall(pattern1, cleaned_str)
            
            # 格式2：电251-45 → 25电1 (专业+年份班级-人数)
            pattern2 = r'([\u4e00-\u9fa5]*)(\d{2})(\d+)(?:-(\d+))?'
            matches2 = re.findall(pattern2, cleaned_str)

            # 处理格式1
            for match in matches1:
                major, year, class_num, count = match
                class_name = f"{year}{major}{class_num}"
                cnt = int(count) if count else None
                classes.append((class_name, cnt))
            
            # 处理格式2 (如果格式1没匹配到，或者有混合情况)
            # 注意：matches2 也会匹配到 matches1 的情况，所以需要去重或逻辑判断
            # 但您的原代码是分开追加的，这里保持原逻辑
            if not matches1: 
                for match in matches2:
                    major, year, class_num, count = match
                    if not major: major = "电" # 默认专业
                    
                    class_name = f"{year}{major}{class_num}"
                    
                    # 避免重复添加 (因为 pattern2 包含了 pattern1 的部分特征)
                    if not any(c[0] == class_name for c in classes):
                        cnt = int(count) if count else None
                        classes.append((class_name, cnt))
            
            # 格式3：纯数字 231-45 → 23电1
            if not classes:
                pattern3 = r'(\d{2})(\d+)(?:-(\d+))?'
                matches3 = re.findall(pattern3, cleaned_str)
                for match in matches3:
                    year, class_num, count = match
                    class_name = f"{year}电{class_num}"
                    if not any(c[0] == class_name for c in classes):
                        cnt = int(count) if count else None
                        classes.append((class_name, cnt))

            # 格式4：三位数字 251 → 25电1
            if not classes:
                pattern4 = r'(\d{3})(?:-(\d+))?'
                matches4 = re.findall(pattern4, cleaned_str)
                for match in matches4:
                    full_num, count = match
                    if len(full_num) == 3:
                        year = full_num[:2]
                        class_num = full_num[2:]
                        class_name = f"{year}电{class_num}"
                        if not any(c[0] == class_name for c in classes):
                            cnt = int(count) if count else None
                            classes.append((class_name, cnt))
                            
            return classes

        # 定义排序函数
        def get_class_sort_key(class_name):
            # 匹配年份+专业+班号
            match = re.search(r'^(\d{2})', str(class_name))
            if match:
                year = int(match.group(1))
                class_num_match = re.search(r'(\d+)$', str(class_name))
                if class_num_match:
                    class_num = int(class_num_match.group(1))
                    return year * 100 + class_num
                return year * 100
            return 999999

        processed_data = []
        for index, row in df_clean.iterrows():
            textbook_name = row['教材名称']
            publisher = row['出版社']
            isbn = row['书号']
            class_info = row['使用班级']
            
            if pd.isna(class_info) or str(class_info).strip() == '':
                continue
            
            classes = parse_class_info_new_format(class_info)
            
            for class_name, student_count in classes:
                processed_data.append({
                    '教材名称': textbook_name,
                    '出版社': publisher,
                    '书号': isbn,
                    '班级': class_name,
                    '人数': student_count
                })

        result_df = pd.DataFrame(processed_data)
        if result_df.empty:
            return {"error": "未能解析出有效数据"}

        # 排序
        result_df['排序键'] = result_df['班级'].apply(get_class_sort_key)
        result_df_sorted = result_df.sort_values('排序键', ascending=True)

        # 去重
        result_df_unique = result_df_sorted.drop_duplicates(subset=['班级', '教材名称', '出版社', '书号']).copy()

        # 编号
        unique_classes = result_df_unique['班级'].drop_duplicates().tolist()
        class_map = {name: i for i, name in enumerate(unique_classes, 1)}
        result_df_unique['编号'] = result_df_unique['班级'].map(class_map)

        # 最终列顺序 (注意：您代码里去掉了排序键)
        final_df = result_df_unique[['编号', '班级', '人数', '教材名称', '出版社', '书号']].reset_index(drop=True)

        # === 保存文件 ===
        filename = f"westlake_final_{uuid.uuid4()}.xlsx"
        save_path = os.path.join("static", filename)
        final_df.to_excel(save_path, index=False)

        # 生成链接
        base_url = str(request.base_url).rstrip("/")
        download_url = f"{base_url}/static/{filename}"
        if download_url.startswith("http://"):
            download_url = download_url.replace("http://", "https://", 1)

        return {"download_url": download_url, "message": "西湖职高(最终版)处理完成"}

    except Exception as e:
        return {"error": f"处理出错: {str(e)}"}
