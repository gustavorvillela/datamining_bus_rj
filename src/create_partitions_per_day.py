from sqlalchemy import create_engine, text
from datetime import timedelta
from tqdm import tqdm

# ====== CONFIGURAÇÕES ======
user = "gis"
password = "password"
host = "localhost"
port = "5432"
database = "gis"

engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

# ====== PASSO 1: Obter intervalo de datas da tabela antiga ======
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT MIN(datahora) AS min_date, MAX(datahora) AS max_date
        FROM onibus_rj;
    """)).fetchone()

min_date, max_date = result
print(f"Intervalo de datas: {min_date} até {max_date}")

# ====== PASSO 2: Gerar lista de datas para partições ======
current_date = min_date.date()
end_date = max_date.date()

partition_dates = []
while current_date <= end_date:
    partition_dates.append(current_date)
    current_date += timedelta(days=1)

print(f"Total de partições a criar: {len(partition_dates)}")

# ====== PASSO 3: Criar partições com tqdm ======
with engine.connect() as conn:
    for date in tqdm(partition_dates, desc="Criando partições"):
        next_date = date + timedelta(days=1)
        partition_name = f"onibus_rj_{date.strftime('%Y%m%d')}"
        create_partition_sql = f"""
        CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF onibus_rj_part
        FOR VALUES FROM ('{date} 00:00:00') TO ('{next_date} 00:00:00');
        """
        conn.execute(text(create_partition_sql))
    conn.commit()

print(f"✅ {len(partition_dates)} partições criadas com sucesso!")
