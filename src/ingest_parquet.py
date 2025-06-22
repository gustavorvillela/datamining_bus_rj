import os
import json
import pandas as pd
from datetime import datetime
from tqdm import tqdm

base_dir = "/media/default/Extreme SSD/Datamining/T3/Train"
output_dir = "/media/default/Extreme SSD/Datamining/T3/Parquets"
os.makedirs(output_dir, exist_ok=True)

def convert_timestamp(ms):
    try:
        return datetime.fromtimestamp(int(ms) / 1000)
    except:
        return None  # Se der erro, retorna None

for root, _, files in os.walk(base_dir):
    for file in tqdm(sorted(files), desc="Convertendo JSONs para Parquet"):
        if file.endswith(".json"):
            json_path = os.path.join(root, file)
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                df = pd.DataFrame(data)
                if df.empty:
                    continue  # Pula arquivos vazios

                # Limpeza e conversões
                df["latitude"] = pd.to_numeric(df["latitude"].str.replace(",", "."), errors='coerce')
                df["longitude"] = pd.to_numeric(df["longitude"].str.replace(",", "."), errors='coerce')
                df["datahora"] = df["datahoraservidor"].apply(convert_timestamp)

                # Drop de linhas inválidas
                df = df.dropna(subset=["latitude", "longitude", "datahora"])

                # Salva como Parquet no SSD
                parquet_filename = file.replace(".json", ".parquet")
                parquet_path = os.path.join(output_dir, parquet_filename)
                df.to_parquet(parquet_path, index=False)

            except Exception as e:
                print(f"[ERRO] Ao processar {file}: {e}")
