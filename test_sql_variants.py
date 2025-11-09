from sqlalchemy import create_engine
import pyodbc

user = "sa"
password = "admin123"
database = "AirbnbBI"

# Variante A: instancia default con driver 17
conn_str_A = f"mssql+pyodbc://{user}:{password}@localhost/{database}?driver=ODBC+Driver+17+for+SQL+Server"

# Variante B: si tu instancia es SQLEXPRESS (nota la doble barra)
conn_str_B = f"mssql+pyodbc://{user}:{password}@localhost\\SQLEXPRESS/{database}?driver=ODBC+Driver+17+for+SQL+Server"

# Variante C: usar ip y puerto (si config. TCP en 1433)
conn_str_C = f"mssql+pyodbc://{user}:{password}@127.0.0.1:1433/{database}?driver=ODBC+Driver+17+for+SQL+Server"

candidates = {'A': conn_str_A, 'B': conn_str_B, 'C': conn_str_C}

for k, cs in candidates.items():
    print("Probando variante", k)
    try:
        engine = create_engine(cs)
        with engine.connect() as conn:
            r = conn.execute("SELECT 1").scalar()
            print("Conexión OK (variante", k, "), resultado:", r)
    except Exception as e:
        print("Fallo variante", k, "->", e)
    print("-"*50)
