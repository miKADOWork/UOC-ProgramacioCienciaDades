# incluirem aqui els diferents moduls que necessitem per la pac-4:
import pandas as pd
import csv
import math
import time as t

# Módul propi
import PandasCustom as pdc
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns # para el ex 4.2

# Cridas a les funcions --------------------------------------------------
PATH_TO_FILE = "./data/TMDB.zip"
PATH_TO_CSV = [
                "./TMDB_info.csv",
                "./TMDB_overview.csv",
                "./TMDB_distribution.csv"]

# Aqui tindrem el bucle principal del programa:
##### Exercici 1 -------------------------------------------------------------------------------------------------------
print("-------------------------------------------------------")
print("Exercici 1:")
print("-------------------------------------------------------")

# Descomprimim el arxiu
pdc.read_file_tz_zp(PATH_TO_FILE)


# Unifiquem els csv amb df
df_result = pdc.unify_n_df(PATH_TO_CSV[0],
                           PATH_TO_CSV[1],
                           PATH_TO_CSV[2])

# Mirem els 10 primers resultats:
print(df_result.head(10))

# Unifiquem els csv amb dicts
dic_result_vis = pdc.unify_n_dict(PATH_TO_CSV[0],
                           PATH_TO_CSV[1],
                           PATH_TO_CSV[2])

print("Per el últim apartat, ens interesa mès df perque esta optimitzat per csv,"
      + "a  part ens permet operar hi treballar mès comodoament que un dict"
      + "a meès, com hem vist els temps de lectura són millors per aquest cas.")

##### Exercici 2 -------------------------------------------------------------------------------------------------------
print("-------------------------------------------------------")
print("Exercici 2:")
print("-------------------------------------------------------")
# A partir d'ara treballarem amb el df_result:

# Afegim una columna
df_result = pdc.addDaysOnAirCol(df_result, "first_air_date", "last_air_date")

# Mostrem els 10 primers valors
print(df_result.head(10))

# Not abaliable exercice
dict_result_22 = pdc.createDictEx22(df_result)

i=0
print("Els 5 primers resultats de las urls dels posters:")
for key in dict_result_22.keys():
    print(dict_result_22[key])
    i += 1
    if i > 5:
        break

# Exercici 3
print("-------------------------------------------------------")
print("Exercici 3:")
print("-------------------------------------------------------")

# Fem una copia del df
df_copy = df_result

df_copy = pdc.filterByReggex(df_copy)
print("Mostrem per pantalla els resultats del ex 3.1")
print(df_copy["name"].tolist())

# Ara fem el exercici 3.2:
# Reseteem la variable
df_copy = df_result

print("Mostrem per pantalla els resultats del ex 3.2")
df_copy = pdc.filterByReggex32(df_copy)
print(df_copy.head(10))

# Resetem novament la variable:
df_copy = df_result

print("Mostrem per pantalla els resultats del ex 3.3")
# Ara filtrem per obtenir les series amb els camps requerits en japones:
df_copy = pdc.filterByReggexJapLang(df_copy)
print(df_copy.head(20))

# Els exercici 4 comenza a partir d'aquest punt:
print("-------------------------------------------------------")
print("Exercici 4:")
print("-------------------------------------------------------")

# creem una variable:
df_ex41 = df_result

# Crearem una columna extra
df_ex41["year_only"] = df_ex41["first_air_date"].dropna().dt.year.astype(int)
count_x_year = df_ex41["year_only"].astype("category").value_counts()

dict_plot = dict({(int(key),value) for key,value in dict(count_x_year).items()})
print(type(dict_plot))

# Deixo aquesta solució ineficient, pero es podria utilitzar
# per comparar resultats
# Cal netejar els NaNs
#list_clean_of_nans = [int(year) for year in df_ex41["year_only"].tolist() if not math.isnan(year)]
#
# Creem un dicionari per guardar els valors
#dict_plot = {(key, pdc.funt_counter(df_result, key)) for key in list(set(list_clean_of_nans))}
#print(dict_plot)
# Fi solució ineficient

# Ara farem el plot
x_values = list(dict_plot.keys())
y_values = list(dict_plot.values())

plt.bar(x_values,
        y_values)

plt.xlabel("anys")
plt.ylabel("# produccions")
plt.title("Produccions per any")
plt.show()

# Per el exercici 4.2
# Primer necesitem treure els "type" que hi ha:
# assignem la var i ja filtro els nans directament:
list_of_types = df_ex41["type"].dropna().tolist()
list_of_types = list(set(list_of_types))

# Agrupem el df per any i tipus
# Per fer-ho fem un llista amb els blocks de 10 anys:
list_10_years = list(range(1940,
                           2021 + 11, 10))

df_ex42 = df_result.dropna()
df_ex42["decada"] = (df_ex42["year_only"] // 10) * 10

# Fem el df amb les contes per tal de fer el plot
df_count = df_ex42.groupby(["decada", "type"]).size().reset_index(name="count")

# Configurem la mida
plt.figure(figsize=(12,6))
sns.lineplot(x="decada",
             y="count",
             hue="type",
             data=df_count,
             marker="o")

# Fem els labels:
plt.xlabel("Decada")
plt.ylabel("# sèries")
plt.title("# Series per tipus i dècada")

# Fiquem la llegenda al seu lloc
plt.legend(title="type", loc="upper left")

# Mostrem el gràfic:
plt.show()

# Apartat 4.3: (INCOMPLET)
# Creem un nou df amb les dades del fusionat:
df_ex43 = df_result

# Calculem el nombre de series totals (que es el nombre de files del df_result)
total_series = df_result.shape[0]

# Contem i calculem els percentatges:
df_ex43["count_of_genres"] = df_ex43["genres"].value_counts().apply(lambda x : x / total_series)
print(df_ex43[df_ex43["count_of_genres"]>0.01])

