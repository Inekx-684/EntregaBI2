from src.extraccion import Extraccion
from src.transformacion import Transformacion
from src.carga import Carga  # 👈 nueva importación

# === FASE DE EXTRACCIÓN ===
ext = Extraccion()
trans = Transformacion()
cargador = Carga()

db = ext.conectar()  # Conexión a MongoDB

if db is not None:
    print("Conectado a la base de datos:", db.name)
    print("Colecciones disponibles:", db.list_collection_names())

    # Obtener datos desde MongoDB
    df_listings = ext.obtenerDataFrame(db, "listings")
    df_reviews = ext.obtenerDataFrame(db, "reviews")
    df_calendar = ext.obtenerDataFrame(db, "calendar")

    print("DataFrames creados correctamente")
    print("Listados:", df_listings.shape)
    print("Reviews:", df_reviews.shape)
    print("Calendario:", df_calendar.shape)

    # === FASE DE TRANSFORMACIÓN ===
    df_listings_transformado, df_reviews_transformado, df_calendar_transformado, resumen = trans.limpiar_datos(
        df_listings, df_reviews, df_calendar
    )

    print("\n=== Resumen de Transformación ===")
    for clave, valor in resumen.items():
        print(f"{clave}: {valor}")

    print("\nTransformación finalizada correctamente.")

    # === FASE DE CARGA ===
    print("\n=== Iniciando fase de CARGA ===")
    df_dict = {
        "listings": df_listings_transformado,
        "reviews": df_reviews_transformado,
        "calendar": df_calendar_transformado
    }

    cargador.guardar_csv(df_dict)  # 👈 guarda los CSV transformados

else:
    print("No se pudo establecer la conexión a MongoDB ❌")

from src.carga import Carga

# Después de la transformación:
carga = Carga()
carga.cargar_a_sqlserver(df_listings, df_reviews, df_calendar)
