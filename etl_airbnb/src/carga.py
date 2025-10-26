import os
import pandas as pd
from datetime import datetime

class Carga:
    def __init__(self):
        # Ruta donde se guardarán los logs
        ruta_actual = os.path.dirname(os.path.abspath(__file__))
        logs_dir = os.path.join(ruta_actual, "..", "logs")
        os.makedirs(logs_dir, exist_ok=True)

        self.log_path = os.path.join(logs_dir, "log.txt")
        if not os.path.exists(self.log_path):
            with open(self.log_path, "w", encoding="utf-8") as f:
                f.write("----- INICIO DE SESIÓN DE LOG (CARGA) -----\n")

    def registrarLog(self, mensaje):
        hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(f"[{hora}] {mensaje}\n")

    def guardar_csv(self, df_dict):
        """
        Guarda los DataFrames transformados en archivos CSV.
        df_dict debe ser un diccionario con la forma:
        {
            'listings': df_listings,
            'reviews': df_reviews,
            'calendar': df_calendar
        }
        """
        try:
            ruta_actual = os.path.dirname(os.path.abspath(__file__))
            carpeta_salida = os.path.join(ruta_actual, "..", "data", "procesado")
            os.makedirs(carpeta_salida, exist_ok=True)

            for nombre, df in df_dict.items():
                if df is not None and not df.empty:
                    ruta_archivo = os.path.join(carpeta_salida, f"{nombre}_procesado.csv")
                    df.to_csv(ruta_archivo, index=False, encoding="utf-8-sig")
                    self.registrarLog(f"Archivo CSV generado correctamente: {ruta_archivo}")
                    print(f"✅ Archivo guardado: {ruta_archivo}")
                else:
                    self.registrarLog(f"⚠️ DataFrame vacío o no encontrado: {nombre}")
                    print(f"⚠️ No se pudo guardar {nombre}: DataFrame vacío")

            print("\n💾 Todos los archivos CSV se han guardado correctamente.")
            return True

        except Exception as e:
            self.registrarLog(f"Error al guardar archivos CSV: {e}")
            print(f"❌ Error al guardar archivos: {e}")
            return False
