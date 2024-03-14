from pyspark.sql import SparkSession

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("Exemple de script PySpark") \
    .getOrCreate()

# Charger le fichier CSV en tant que DataFrame
# Lire le fichier CSV en utilisant des barres obliques normal
# Lire le fichier CSV en utilisant des barres obliques inverses échappées
df = spark.read.csv("C:\\Users\\MouhamethSECK\\Downloads\\archive (2)\\supermarket_sales - Sheet1.csv", header=True, inferSchema=True)


# Afficher le schéma du DataFrame
df.printSchema()

# Afficher les premières lignes du DataFrame
df.show()

# Effectuer des transformations sur le DataFrame
# Filtrer les données où la colonne "City" est égale à "Mandalay"
df_mandalay = df.filter(df["City"] == "Mandalay")

df_mandalay.show() 


# Effectuer des transformations sur le DataFrame
df_mandalay = df.filter(df["City"] == "Mandalay")




