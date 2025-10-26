import pandas as pd
import os
from datetime import datetime


class Transformacion:
    """
    Clase que realiza la transformación y limpieza de los datos
    provenientes de las colecciones de Airbnb (listings, reviews, calendar).

    Incluye:
    - Eliminación de duplicados y valores nulos.
    - Normalización de precios.
    - Conversión de fechas a formato datetime.
    - Derivación de variables de tiempo (año, mes, día, trimestre).
    - Registro detallado en logs.
    """

    def __init__(self):
        ruta_actual = os.path.dirname(os.path.abspath(__file__))
        logs_dir = os.path.join(ruta_actual, "..", "logs")
        os.makedirs(logs_dir, exist_ok=True)
        self.log_path = os.path.join(logs_dir, "log.txt")

    def registrarLog(self, mensaje: str):
        """Guarda mensajes con timestamp en el archivo de logs."""
        hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(f"[{hora}] {mensaje}\n")

    def limpiar_datos(self, df_listings, df_reviews, df_calendar):
        """
        Limpia y transforma los DataFrames recibidos.

        Args:
            df_listings (pd.DataFrame): Datos de alojamientos.
            df_reviews (pd.DataFrame): Datos de reseñas.
            df_calendar (pd.DataFrame): Datos de calendario.

        Returns:
            tuple: (df_listings_limpio, df_reviews_limpio, df_calendar_limpio, resumen)
        """

        resumen = {}

        try:
            self.registrarLog("Inicio de transformación de datos")

            # LIMPIEZA DE DUPLICADOS
            # =======================
            resumen["listings_registros_iniciales"] = len(df_listings)
            resumen["reviews_registros_iniciales"] = len(df_reviews)
            resumen["calendar_registros_iniciales"] = len(df_calendar)

            df_listings = df_listings.drop_duplicates(subset="_id", keep="first")
            df_reviews = df_reviews.drop_duplicates(subset="id", keep="first")
            df_calendar = df_calendar.drop_duplicates(subset=["listing_id", "date"], keep="first")

            resumen["listings_registros_sin_duplicados"] = len(df_listings)
            resumen["reviews_registros_sin_duplicados"] = len(df_reviews)
            resumen["calendar_registros_sin_duplicados"] = len(df_calendar)

            # LIMPIEZA Y NORMALIZACIÓN
            # =======================
            columnas_utiles = ["_id", "name", "host_id", "host_name", "neighbourhood", "price"]
            df_listings = df_listings[[col for col in columnas_utiles if col in df_listings.columns]]

            if "price" in df_listings.columns:
                df_listings["price"] = (
                    df_listings["price"]
                    .astype(str)
                    .str.replace(r"[\$,]", "", regex=True)
                    .replace("", None)
                    .astype(float)
                )

            # CONVERSIÓN DE FECHAS
            # =======================
            if "date" in df_calendar.columns:
                df_calendar["date"] = pd.to_datetime(df_calendar["date"], errors="coerce")

                df_calendar["year"] = df_calendar["date"].dt.year
                df_calendar["month"] = df_calendar["date"].dt.month
                df_calendar["day"] = df_calendar["date"].dt.day
                df_calendar["quarter"] = df_calendar["date"].dt.quarter

            if "date" in df_reviews.columns:
                df_reviews["date"] = pd.to_datetime(df_reviews["date"], errors="coerce")
                df_reviews["year"] = df_reviews["date"].dt.year
                df_reviews["month"] = df_reviews["date"].dt.month


            # CATEGORIZACIÓN DE PRECIOS
            # =======================
            if "price" in df_listings.columns:
                df_listings["price_category"] = pd.cut(
                    df_listings["price"],
                    bins=[0, 100, 300, 1000, float("inf")],
                    labels=["Bajo", "Medio", "Alto", "Lujo"]
                )

            # REGISTRO Y RESUMEN
            # =======================
            resumen["listings_columnas_finales"] = list(df_listings.columns)
            resumen["reviews_columnas_finales"] = list(df_reviews.columns)
            resumen["calendar_columnas_finales"] = list(df_calendar.columns)
            resumen["listings_precio_promedio"] = df_listings["price"].mean() if "price" in df_listings.columns else None

            self.registrarLog("Transformación completada correctamente")
            self.registrarLog(f"Resumen: {resumen}")

            return df_listings, df_reviews, df_calendar, resumen

        except Exception as e:
            self.registrarLog(f"Error en la transformación: {e}")
            resumen["error"] = str(e)
            return df_listings, df_reviews, df_calendar, resumen
