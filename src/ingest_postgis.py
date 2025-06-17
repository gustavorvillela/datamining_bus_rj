import os
import json
import psycopg2
from datetime import datetime
from tqdm import tqdm

# Caminho para os arquivos JSON
base_dir = "/media/default/Extreme SSD/Datamining/T3/Train"

# Conexão com o banco PostgreSQL com PostGIS
conn = psycopg2.connect(
    dbname="gis",
    user="gis",
    password="password",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

def convert_timestamp(ms):
    return datetime.fromtimestamp(int(ms) / 1000)

# Coletar todos os arquivos JSON
json_files = []
for root, _, files in os.walk(base_dir):
    for file in files:
        if file.endswith(".json"):
            json_files.append(os.path.join(root, file))

# Loop externo: arquivos
for json_file in tqdm(sorted(json_files), desc="Arquivos JSON", unit="arquivo"):
    with open(json_file, 'r', encoding='utf-8') as f:
        try:
            dados = json.load(f)
        except Exception as e:
            tqdm.write(f"Erro lendo {json_file}: {e}")
            continue

        # Loop interno: registros no JSON
        for registro in tqdm(dados, desc=f"Registros em {os.path.basename(json_file)}", leave=False, unit="registro"):
            try:
                ordem = registro["ordem"]
                linha = registro["linha"]
                velocidade = int(registro["velocidade"])

                # Substitui vírgulas por pontos
                lat = float(registro["latitude"].replace(",", "."))
                lon = float(registro["longitude"].replace(",", "."))

                datahora = convert_timestamp(registro["datahoraservidor"])

                cursor.execute("""
                    INSERT INTO onibus_rj (ordem, linha, velocidade, datahora, geom)
                    VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                """, (ordem, linha, velocidade, datahora, lon, lat))

            except Exception as e:
                tqdm.write(f"Erro no registro {registro}: {e}")
                continue

    conn.commit()

cursor.close()
conn.close()
