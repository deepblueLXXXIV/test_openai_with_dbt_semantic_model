import json 
import duckdb 
from dbt.contracts.graph.manifest  import Manifest  # 修正导入路径 

from langchain_openai import OpenAI 
from langchain.chains import LLMChain 
from langchain.prompts  import PromptTemplate 
import yaml 
from typing import Dict, List 
 
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
 
 
def load_dbt_profile(profile_path) -> Dict:
    """从 ~/.dbt/profiles.yml  加载DuckDB配置"""
    with open(profile_path,  "r") as f:
        profiles = yaml.safe_load(f) 
        duckdb_config = profiles["project_name"]["outputs"]["dev"]
        return duckdb_config 

# 配置参数 
DBT_PROJECT_DIR = "./"
MANIFEST_PATH = f"./target/manifest.json" 
PROFILE_PATH = f"../.dbt/profiles.yml" 

config = load_dbt_profile(PROFILE_PATH)
# 初始化DuckDB连接 
conn = duckdb.connect(database=config["path"]) 
 
# 正确加载 manifest 文件 [3]()
def load_manifest(file_path: str) -> Manifest:
    """从 JSON 文件加载 manifest 对象"""
    with open(file_path, 'r') as f:
        manifest_dict = json.load(f) 
    return Manifest.from_dict(manifest_dict)   # 使用 from_dict 方法
 
# 加载语义模型
manifest = load_manifest(MANIFEST_PATH)

semantic_model = manifest.semantic_models["semantic_model.project_name.orders"] 

staging_orders = manifest.nodes["model.project_name.stg_orders"]
staging_customers = manifest.nodes["model.project_name.stg_customers"]

# 自然语言处理模板
prompt_template = """
You are a SQL expert with access to dbt semantic model:
- semantic_model: {semantic_model}
- metrics: {metrics}
- subquery: {subquery}
 
Translate user question into DuckDB SQL using the semantic layer
subquery part should be used as the source

User question: {question}

The return result should be plain sql without thinking process, sql should not have ``` prefix or suffix

"""
 
# 初始化语言模型
llm = OpenAI(temperature=0)
prompt = ChatPromptTemplate.from_template(template=prompt_template)
model = ChatOpenAI(model="gpt-4.1-mini")
chain = prompt | model | StrOutputParser() 

def execute_query(sql):
    """执行查询并返回结果"""
    result = conn.execute(sql).fetchall() 
    return result
 
def semantic_qa(user_question):
    """语义问答核心函数"""
    
    input_data = {
        'semantic_model': semantic_model,
        'metrics': manifest.metrics,
        'subquery': staging_orders.raw_code + ' ' + staging_customers.raw_code,
        'question': user_question
    }
    
    sql = chain.invoke(input=input_data)
    
    print(sql)
    return execute_query(sql)
 
if __name__ == "__main__":
    print("dbt 语义层问答系统 | 输入 'exit' 退出")
    while True:
        question = input("\n问题：")
        if question.lower()  == "exit":
            break
        
        try:
            results = semantic_qa(question)
            print("\n结果：")
            for row in results:
                print(row)
        except Exception as e:
            print(f"错误：{str(e)}")