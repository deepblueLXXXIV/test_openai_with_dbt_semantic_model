import json
from pathlib import Path
from dbt.parser.manifest  import ManifestLoader
from dbt.config.runtime  import RuntimeConfig
 
def load_dbt_manifest(project_dir: str):
    """
    加载并解析 dbt manifest 文件
    
    Args:
        project_dir: dbt 项目根目录路径
        
    Returns:
        Manifest 对象
    """
    print(project_dir)
    try:
        # 构建完整路径
        manifest_path = Path(project_dir) / "target" / "manifest.json" 
        
        if not manifest_path.exists(): 
            raise FileNotFoundError(f"未找到 manifest 文件: {manifest_path}")
            
        # 创建运行时配置
        runtime_config = RuntimeConfig.load_root(project_dir=DBT_PROJECT_DIR) 
        
        # 使用 ManifestLoader 加载
        manifest = ManifestLoader.get_full_manifest( 
            runtime_config=runtime_config,
            manifest_json=json.loads(manifest_path.read_text()) 
        )
        
        return manifest
        
    except Exception as e:
        print(f"加载 manifest 时出错: {str(e)}")
        return None
 
# 使用示例
if __name__ == "__main__":
    PROJECT_DIR = "C:/Users/tonys/project_name"  # 替换为实际项目路径 
    
    manifest = load_dbt_manifest(PROJECT_DIR)
    
    if manifest:
        print("成功加载 manifest 文件！")
        print(f"包含 {len(manifest.nodes)}  个节点")
        print(f"包含 {len(manifest.semantic_models)}  个语义模型")
        
        # 示例：访问特定语义模型
        if "orders" in manifest.semantic_models: 
            orders_model = manifest.semantic_models["orders"] 
            print("\nOrders 语义模型信息:")
            print(f"描述: {orders_model.description}") 
            print(f"包含 {len(orders_model.dimensions)}  个维度")
            print(f"包含 {len(orders_model.measures)}  个指标")
    else:
        print("无法加载 manifest 文件，请检查错误信息")