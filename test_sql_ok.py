from sqlalchemy import create_engine, text

user = "sa"          # o tu usuario de SQL Server
password = "admin123"  # la contraseña que configuraste
database = "master"    # puedes usar master para probar

conn_str = f"mssql+pyodbc://{user}:{password}@localhost\\SQLEXPRESS/{database}?driver=ODBC+Driver+17+for+SQL+Server"

try:
    engine = create_engine(conn_str)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM sys.databases"))
        print("✅ Conexión establecida correctamente. Bases de datos disponibles:")
        for row in result:
            print(" -", row[0])
except Exception as e:
    print("❌ Error al conectar:", e)
