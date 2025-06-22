from sqlalchemy import create_engine, text
from datetime import date, timedelta
from tqdm import tqdm

# Configurações do banco
user = "gis"
password = "password"
host = "localhost"
port = "5432"
database = "gis"

engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

# Datas do intervalo - ajuste conforme seu caso
start_date = date(2024, 4, 25)
end_date = date(2024, 5, 11)

# Linhas desejadas (string formatada para SQL IN)
linhas_desejadas = (
    "'483', '864', '639', '3', '309', '774', '629', '371', '397', '100', '838', '315', "
    "'624', '388', '918', '665', '328', '497', '878', '355', '138', '606', '457', '550', "
    "'803', '917', '638', '2336', '399', '298', '867', '553', '565', '422', '756', '292', "
    "'554', '634', '232', '415', '2803', '324', '852', '557', '759', '343', '779', '905', '108'"
)

with engine.connect() as conn:
    current_date = start_date
    while current_date <= end_date:
        next_date = current_date + timedelta(days=1)
        
        delete_sql = text(f"""
            DELETE FROM onibus_rj
            WHERE linha NOT IN ({linhas_desejadas})
            AND datahora >= '{current_date} 00:00:00' 
            AND datahora < '{next_date} 00:00:00';
        """)
        
        result = conn.execute(delete_sql)
        conn.commit()
        
        print(f"Deletado dia {current_date} - Linhas removidas: {result.rowcount}")
        
        current_date = next_date
