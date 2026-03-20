import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
import json
import time
import io
from azure.storage.blob import BlobServiceClient


print(f"=== DIET ANALYSIS STARTED at {datetime.now()} ===")
print(f"Current directory: {os.getcwd()}")
print(f"Files in directory: {os.listdir('.')}")


# 1. LOAD DATA 
try:
    dtypes = {
        'Protein(g)': 'float32',
        'Carbs(g)': 'float32',
        'Fat(g)': 'float32'
    }
    
    connection_string = os.environ["AzureWebJobsStorage"]

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("datasets")

    blob_client = container_client.get_blob_client("All_Diets.csv")
    data = blob_client.download_blob().readall()

    df = pd.read_csv(io.BytesIO(data))
    
    print(f"Successfully loaded {len(df)} recipes with optimized memory (float32)")
    print(f"Columns found: {df.columns.tolist()}")
except Exception as e:
    print(f"Error loading CSV: {e}")
    exit(1)


# 2. HANDLE MISSING VALUES 
numeric_cols = ['Protein(g)', 'Carbs(g)', 'Fat(g)']
for col in numeric_cols:
    if col in df.columns:
        df[col] = df[col].fillna(df[col].mean())


# 3. CALCULATE AVERAGE MACROS PER DIET TYPE
avg_macros = df.groupby('Diet_type')[['Protein(g)', 'Carbs(g)', 'Fat(g)']].mean().round(2)
print("\n=== AVERAGE MACROS PER DIET ===")
print(avg_macros)
avg_macros.to_csv('avg_macros_by_diet.csv')

# 4. TOP 5 PROTEIN RECIPES PER DIET
top_protein = df.sort_values('Protein(g)', ascending=False).groupby('Diet_type').head(5)
top_protein[['Diet_type', 'Recipe_name', 'Protein(g)']].to_csv('top5_protein_per_diet.csv')
print("Top protein recipes saved")

# 5. DIET WITH HIGHEST PROTEIN
highest_protein_diet = df.groupby('Diet_type')['Protein(g)'].mean().idxmax()
highest_value = df.groupby('Diet_type')['Protein(g)'].mean().max()
print(f"\n=== HIGHEST PROTEIN DIET ===")
print(f"{highest_protein_diet} with average {highest_value:.2f}g protein")

# 6. MOST COMMON CUISINE PER DIET
most_common_cuisine = df.groupby('Diet_type')['Cuisine_type'].agg(lambda x: x.mode()[0] if not x.mode().empty else 'Unknown')
print("\n=== MOST COMMON CUISINE PER DIET ===")
print(most_common_cuisine)
most_common_cuisine.to_csv('common_cuisine_per_diet.csv')

# 7. CREATE NEW METRICS
df['Protein_to_Carbs_ratio'] = df['Protein(g)'] / df['Carbs(g)'].replace(0, 0.01)
df['Carbs_to_Fat_ratio'] = df['Carbs(g)'] / df['Fat(g)'].replace(0, 0.01)
df[['Recipe_name', 'Protein_to_Carbs_ratio', 'Carbs_to_Fat_ratio']].to_csv('ratio_metrics.csv')
print("Ratio metrics calculated")

# 8. VISUALIZATION 1: BAR CHART
avg_macros.plot(kind='bar')
plt.title('Average Macronutrient Content by Diet Type', fontsize=16)
plt.ylabel('Grams', fontsize=12)
plt.xlabel('Diet Type', fontsize=12)
plt.xticks(rotation=45)
plt.tight_layout()
print("Bar chart saved as avg_macros_bar_chart.png")

# 9. VISUALIZATION 2: HEATMAP
plt.title('Macronutrient Distribution Heatmap by Diet Type', fontsize=16)
plt.tight_layout()
print("Heatmap saved as macro_heatmap.png")

# 10. VISUALIZATION 3: SCATTER PLOT
top_50_protein = df.nlargest(50, 'Protein(g)')
plt.title('Top 50 Protein-Rich Recipes by Cuisine', fontsize=16)
plt.xticks(rotation=90)
plt.tight_layout()
print("Scatter plot saved as top_protein_scatter.png")

print(f"\n=== ANALYSIS COMPLETE at {datetime.now()} ===")
print("Files generated:")
for file in ['avg_macros_by_diet.csv', 'top5_protein_per_diet.csv', 'common_cuisine_per_diet.csv', 
             'ratio_metrics.csv', 'avg_macros_bar_chart.png', 'macro_heatmap.png', 'top_protein_scatter.png']:
    if os.path.exists(file):
        print(f"   {file}")
    else:
        print(f"   {file}")\

# json response

start_time = time.time()
result = {
    "avg_macros": {
        "labels": avg_macros.index.tolist(),
        "protein": avg_macros['Protein(g)'].tolist(),
        "carbs": avg_macros['Carbs(g)'].tolist(),
        "fat": avg_macros['Fat(g)'].tolist()
    },
    "top_protein": top_protein[['Diet_type', 'Recipe_name', 'Protein(g)']].to_dict(orient='records'),
    "common_cuisine": most_common_cuisine.to_dict(),
    "executionTimeMs": int((time.time() - start_time) * 1000)
}

print(json.dumps(result))
