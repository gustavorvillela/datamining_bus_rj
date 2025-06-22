from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://postgres:postgres@localhost:5432/onibus_rj')

with engine.connect() as connection:
    df = pd.read_sql("""
        SELECT linha, COUNT(*) AS total
        FROM onibus_rj
        GROUP BY linha
        ORDER BY total DESC
        LIMIT 10;
    """, con=connection)  # Note o parâmetro 'con='

print(df)