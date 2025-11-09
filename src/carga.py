import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

class Carga:
    def __init__(self):
        # === Crear carpeta logs si no existe ===
        ruta_actual = os.path.dirname(os.path.abspath(__file__))
        logs_dir = os.path.join(ruta_actual, "..", "logs")
        os.makedirs(logs_dir, exist_ok=True)

        # Archivo log
        self.log_path = os.path.join(logs_dir, "log_carga.txt")
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(f"\n----- NUEVA SESIÓN DE CARGA: {datetime.now()} -----\n")

    def registrarLog(self, mensaje):
        hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(f"[{hora}] {mensaje}\n")

    # =====================================================
    # 1️⃣ Guardar archivos Excel (respaldo local)
    # =====================================================
    def guardar_csv(self, df_dict):
        salida_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "salidas")
        os.makedirs(salida_dir, exist_ok=True)

        for nombre, df in df_dict.items():
            try:
                ruta = os.path.join(salida_dir, f"{nombre}.xlsx")
                df.to_excel(ruta, index=False)
                print(f"✅ Archivo {nombre}.xlsx creado ({len(df)} filas)")
                self.registrarLog(f"Archivo {nombre}.xlsx guardado correctamente con {len(df)} registros.")
            except Exception as e:
                print(f"❌ Error al guardar {nombre}: {e}")
                self.registrarLog(f"Error al guardar {nombre}: {e}")

    # =====================================================
    # 2️⃣ Cargar a SQL Server
    # =====================================================
    def cargar_a_sqlserver(self, df_listings, df_reviews, df_calendar):
        try:
            # Parámetros de conexión a SQL Server
            servidor = r"DESKTOP-UMDSLQR\SQLEXPRESS"
            base_datos = "AirbnbBI"
            usuario = "sa"
            contrasena = "admin123"

            # Crear el motor de conexión
            engine = create_engine(
                f"mssql+pyodbc://{usuario}:{contrasena}@{servidor}/{base_datos}?driver=ODBC+Driver+17+for+SQL+Server"
            )

            print("🔄 Cargando DataFrames a SQL Server...")

            # === Convertir columnas incompatibles ===
            def limpiar_dataframe(df):
                # Convertir ObjectId y listas a texto
                for col in df.columns:
                    df[col] = df[col].apply(
                        lambda x: str(x) if isinstance(x, (list, dict)) or "ObjectId" in str(type(x)) else x
                    )
                return df

            df_listings = limpiar_dataframe(df_listings)
            df_reviews = limpiar_dataframe(df_reviews)
            df_calendar = limpiar_dataframe(df_calendar)

            # === Subir las tablas al SQL Server ===
            df_listings.to_sql("listings", con=engine, if_exists="replace", index=False)
            df_reviews.to_sql("reviews", con=engine, if_exists="replace", index=False)
            df_calendar.to_sql("calendar", con=engine, if_exists="replace", index=False)

            print("✅ Carga completada correctamente en SQL Server.")
            self.registrarLog("Carga exitosa a SQL Server en la base AirbnbBI.")

        except Exception as e:
            print(f"❌ Error al cargar en SQL Server: {e}")
            self.registrarLog(f"Error al cargar en SQL Server: {e}")
