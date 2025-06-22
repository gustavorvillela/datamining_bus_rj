from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from tqdm import tqdm

# Configurações da conexão
user = "gis"
password = "password"
host = "localhost"
port = "5432"
database = "gis"

engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

start_date = datetime.strptime("2024-04-25", "%Y-%m-%d").date()
end_date = datetime.strptime("2024-05-11", "%Y-%m-%d").date()

partition_dates = []
current_date = start_date
while current_date <= end_date:
    partition_dates.append(current_date)
    current_date += timedelta(days=1)

print(f"Total de partições a criar: {len(partition_dates)}")

with engine.connect() as conn:
    for date in tqdm(partition_dates, desc="Criando partições"):
        next_date = date + timedelta(days=1)
        partition_name = f"onibus_rj_{date.strftime('%Y%m%d')}"
        sql = f"""
        CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF onibus_rj
        FOR VALUES FROM ('{date} 00:00:00') TO ('{next_date} 00:00:00');
        """
        conn.execute(text(sql))
    conn.commit()

print("✅ Partições criadas com sucesso!")
