from pymongo import MongoClient
import pandas as pd
import os
from datetime import datetime

# Obtener datos de MongoDB
class Extraccion:
    def __init__(self):
        # Crear carpeta logs si no existe
        ruta_actual = os.path.dirname(os.path.abspath(__file__))
        logs_dir = os.path.join(ruta_actual, "..", "logs")
        os.makedirs(logs_dir, exist_ok=True)

        # Ruta del archivo log
        self.log_path = os.path.join(logs_dir, "log.txt")

        # Crear log si no existe
        if not os.path.exists(self.log_path):
            with open(self.log_path, "w", encoding="utf-8") as f:
                f.write("----- INICIO DE SESIÓN DE LOG -----\n")

    def registrarLog(self, mensaje):
        """Registra mensajes con fecha y hora en el log."""
        hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(f"[{hora}] {mensaje}\n")

    def conectar(self):
        """Conecta a MongoDB (autenticación incluida)."""
        try:
            # 🔹 Datos de conexión (puedes modificarlos según tu contenedor Docker)
            nombreDb = "airbnb"
            usuario = "admin"
            contrasena = "admin123"

            # 🔹 URI para conexión
            uri = f"mongodb://{usuario}:{contrasena}@localhost:27017/?authSource=admin"

            # 🔹 Crear cliente y conectar
            client = MongoClient(uri)
            db = client[nombreDb]

            # 🔹 Verificar conexión
            colecciones = db.list_collection_names()
            print(f"✅ Conectado a MongoDB (base: {nombreDb})")
            print(f"📦 Colecciones encontradas: {colecciones}")

            self.registrarLog(f"Conexión establecida a MongoDB en base '{nombreDb}' con usuario '{usuario}' ✅")
            return db

        except Exception as e:
            self.registrarLog(f"Conexión fallida a MongoDB ❌ Razón: {e}")
            print("⚠️ No se pudo conectar a MongoDB. Verifica si el contenedor está en ejecución o las credenciales son correctas.")
            return None

    def obtenerDataFrame(self, db, nombreColeccion):
        """Convierte una colección MongoDB a un DataFrame."""
        try:
            coleccion = db[nombreColeccion]
            datos = list(coleccion.find())

            if not datos:
                self.registrarLog(f"Colección '{nombreColeccion}' vacía o no encontrada.")
                return pd.DataFrame()

            df = pd.DataFrame(datos)
            self.registrarLog(f"Colección '{nombreColeccion}' extraída. Registros: {len(df)}")
            return df

        except Exception as e:
            self.registrarLog(f"Error al extraer '{nombreColeccion}': {e}")
            return pd.DataFrame()
