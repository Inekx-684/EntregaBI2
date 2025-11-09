from sqlalchemy import create_engine

servidor = "DESKTOP-UMDSLQR\\SQLEXPRESS"
base_datos = "AirbnbBI"
usuario = "sa"
contrasena = "admin123"
driver = "ODBC Driver 17 for SQL Server"

cadena_conexion = f"mssql+pyodbc://{usuario}:{contrasena}@{servidor}/{base_datos}?driver={driver}"
print("Probando conexión con:", cadena_conexion)

try:
    engine = create_engine(cadena_conexion)
    with engine.connect() as conn:
        print("✅ Conexión exitosa con SQL Server")
except Exception as e:
    print("❌ Error al conectar:", e)
