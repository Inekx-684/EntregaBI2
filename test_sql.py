from sqlalchemy import create_engine
# Ajusta user/password/server/database según tu SQL Server
user = "sa"
password = "admin123"
server = "localhost"
database = "AirbnbBI"

conn_str = f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(conn_str)

with engine.connect() as conn:
    res = conn.execute("SELECT 1")
    print("Conexión OK, resultado:", res.scalar())
