import os
import json
import psycopg2
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# CONFIGURAÇÕES
base_dir = "/media/default/Extreme SSD/Datamining/T3/Train/2024-05-10"
n_threads = 2  # ajuste conforme CPU / I/O

db_config = {
    'dbname': "gis",
    'user': "gis",
    'password': "password",
    'host': "localhost",
    'port': "5432"
}

def convert_timestamp(ms):
    return datetime.fromtimestamp(int(ms) / 1000)

def process_file(json_file):
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        batch = []
        batch_size = 1000

        with open(json_file, 'r', encoding='utf-8') as f:
            dados = json.load(f)

        for registro in dados:
            try:
                ordem = registro["ordem"]
                linha = registro["linha"]
                velocidade = int(registro["velocidade"])
                lat = float(registro["latitude"].replace(",", "."))
                lon = float(registro["longitude"].replace(",", "."))
                datahora = convert_timestamp(registro["datahoraservidor"])

                batch.append((ordem, linha, velocidade, datahora, lon, lat))

                if len(batch) >= batch_size:
                    args_str = ",".join(cursor.mogrify("(%s,%s,%s,%s,ST_SetSRID(ST_MakePoint(%s,%s),4326))", x).decode('utf-8') for x in batch)
                    cursor.execute("INSERT INTO onibus_rj (ordem, linha, velocidade, datahora, geom) VALUES " + args_str)
                    conn.commit()
                    batch.clear()

            except Exception as e:
                # Não dá rollback aqui para não abortar toda a transação
                print(f"[ERRO] Registro com erro no arquivo {json_file}: {e}")
                continue
        
        # Insere o que sobrou no batch
        if batch:
            args_str = ",".join(cursor.mogrify("(%s,%s,%s,%s,ST_SetSRID(ST_MakePoint(%s,%s),4326))", x).decode('utf-8') for x in batch)
            cursor.execute("INSERT INTO onibus_rj (ordem, linha, velocidade, datahora, geom) VALUES " + args_str)
            conn.commit()

        cursor.close()
        conn.close()
        return f"✔ Processado: {json_file}"

    except Exception as e:
        return f"[ERRO] Falha no arquivo {json_file}: {e}"

# Lista todos os arquivos JSON
json_files = []
for root, _, files in os.walk(base_dir):
    for file in files:
        if file.endswith(".json"):
            json_files.append(os.path.join(root, file))

# Rodar importação com threads
with ThreadPoolExecutor(max_workers=n_threads) as executor:
    futures = {executor.submit(process_file, jf): jf for jf in sorted(json_files)}

    for future in tqdm(as_completed(futures), total=len(futures), desc="Processando arquivos"):
        resultado = future.result()
        tqdm.write(resultado)
