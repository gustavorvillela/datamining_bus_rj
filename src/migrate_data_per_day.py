from sqlalchemy import create_engine, text
from datetime import timedelta
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ====== CONFIGURAÇÃO DA CONEXÃO ======
user = "gis"
password = "password"
host = "localhost"
port = "5432"
database = "gis"

engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}", pool_size=10)

# ====== PASSO 1: Obter intervalo de datas ======
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT MIN(datahora) AS min_date, MAX(datahora) AS max_date
        FROM onibus_rj;
    """)).fetchone()

min_date, max_date = result
print(f"Intervalo de datas: {min_date} até {max_date}")

# ====== PASSO 2: Lista de dias para migrar ======
current_date = min_date.date()
end_date = max_date.date()
all_days = []
while current_date <= end_date:
    all_days.append(current_date)
    current_date += timedelta(days=1)

print(f"Total de dias no intervalo: {len(all_days)}")

# ====== PASSO 3: Função para verificar e migrar cada dia ======
def migrate_day_if_needed(day):
    next_day = day + timedelta(days=1)
    try:
        with engine.connect() as conn:
            # Verificar se a partição já tem dados
            check_sql = text(f"""
                SELECT COUNT(*) FROM onibus_rj_part
                WHERE datahora >= '{day} 00:00:00' AND datahora < '{next_day} 00:00:00';
            """)
            count = conn.execute(check_sql).scalar()

            if count > 0:
                return f"{day}: SKIPPED (já contém {count} linhas)"
            else:
                # Migrar os dados desse dia
                insert_sql = text(f"""
                    INSERT INTO onibus_rj_part (ordem, linha, velocidade, datahora, geom)
                    SELECT ordem, linha, velocidade, datahora, geom
                    FROM onibus_rj
                    WHERE datahora >= '{day} 00:00:00' AND datahora < '{next_day} 00:00:00';
                """)
                conn.execute(insert_sql)
                conn.commit()
                return f"{day}: OK (dados migrados)"
    except Exception as e:
        return f"{day}: ERRO - {str(e)}"

# ====== PASSO 4: Paralelizar com segurança ======
max_workers = 2  # Ajuste conforme sua máquina

results = []
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(migrate_day_if_needed, day): day for day in all_days}
    for future in tqdm(as_completed(futures), total=len(futures), desc="Migrando dias"):
        results.append(future.result())

# ====== PASSO 5: Log final ======
for r in results:
    print(r)

print("✅ Migração segura concluída!")
