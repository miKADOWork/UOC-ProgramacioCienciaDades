# Aquest package guardarà tot el relacionat amb df.
# Els imports per aquest modul son:
import os
import zipfile as zf
import glob
import re
import pandas as pd
import time
import csv
from requests import head

# Variables glob:
reg_extension_file = r"\.[\.\w]"
extensions_allowed = [".zip" ".tar.gz"]
reg2 = r"mystery"
reg3 = r"crime"


def read_file_tz_zp(path):
    """
    Si tenim l'extensió correcta (zip o targz)
    :param path: path al archiu que volem descomprimir
    :return: void pero descomprimeix l'arxiu del path
    """
    # Mirem el tipus d'arxiu
    file_extension = str(re.search(reg_extension_file, path).group())

    if file_extension in extensions_allowed:
        print("La extensió no es correcta")
        print(file_extension)
    else:
        # Descomprimim el fitxer
        with zf.ZipFile(path, 'r') as topsecret:
            topsecret.extractall()


def unify_n_df(*dfListPath):
    """
    :param argsPath: les direccions dels csv que volem
    fusionar en un df
    :return: un únic df
    """
    intial_time = time.time()
    dfList = []
    for path in dfListPath:
        df = pd.read_csv(path,
                          sep=",",
                          lineterminator="\n",
                          encoding="utf-8")

        dfList.append(df)

    df_result = dfList[0]
    # Fem els JOINS per qualsevol nombre de df
    for i in range(1, len(dfList)):
        df_result = pd.merge(df_result, dfList[i], how='inner',
                             left_on=["id"],
                             right_on=["id"],
                             suffixes=("", " "))

    # Eliminemm el sufix de les columnes
    list_keys = [key.strip() for key in df_result.keys()]

    # Per si de cas eliminem possibles espais despres del merge
    df_result.rename(columns={key + " ": key for key in list_keys}, inplace=True)

    print("El temps total d'executar la funct <unify_n_df> és:\n\t{} seg".format(time.time()-intial_time))
    return df_result


def unify_n_dict(*csvListPath):
    """
    Fusionem csv en un dict
    :param csvListPath: Entren els paths dels csv
    :return: retornem un dict
    """
    intial_time = time.time()
    dictList = []

    for path in csvListPath:
        dict_to_save = {}

        with open(path, "r") as csvFile:
            csv_llegit = csv.DictReader(csvFile)

            # Fem que les claus dels dicts siguin les columnes del csv
            for row in csv_llegit:
                for column, value in row.items():
                    if column not in dict_to_save:
                        dict_to_save[column] = []
                    dict_to_save[column].append(value)
        dictList.append(dict_to_save)

        # Imprimim el temps que ha trigat en executar-se
        print(path + " " + "llegit correctament")

    # Fem els merge
    dict_result = dictList[0] | dictList[1]
    dict_present = dict_result | dictList[2]

    print("El temps total d'executar la funct <unify_n_df> és:\n\t{} seg".format(time.time() - intial_time))

    return dict_present


def addDaysOnAirCol(df, init_date_name, last_date_name):
    """
    :param df: el df que modificarem
    :param init_date_name: str amb la data d'inici
    :param last_date_name: str amb la data de fi
    :return: df retorna amb columna extra de dies en emisó.
    """
    df[init_date_name] = pd.to_datetime(df[init_date_name])
    df[last_date_name] = pd.to_datetime(df[last_date_name])
    df["air_days"] = df[last_date_name] - df[init_date_name]
    df = df.sort_values(by="air_days")

    return df


def createDictEx22(df):
    """
    Crea un diccionari dels links als posters (si hi ha sino,
    el value del dict és NO AVALIABLE. Les keys son noms de les sèries.
    :param df: df d'entrada amb les dades
    :return: dict
    """
    # Purguem de nans i fiquem Not avaliable.
    df["homepage"] = df["homepage"].apply(lambda x: "NOT AVALIABLE" if (x == "" or pd.isna(x)) else x)
    df["poster_path\r"] = df["poster_path\r"].apply(lambda x: "NOT AVALIABLE" if (x == "" or pd.isna(x)) else x)

    # Ara fem que la condició (que actualment no te en compte 'altre columna, tingui en compte si l'latre ja es non avalible
    # es fara buida si l'altre ja te el not abaliable per cuan fem la concatenació.
    df.loc[df["homepage"] == "NOT AVALIABLE", "poster_path\r"] = ""
    df.loc[df["poster_path\r"] == "NOT AVALIABLE", "homepage"] = ""

    return_dict = dict(zip(df["name"],
                           df["homepage"] + df["poster_path\r"]))
    return return_dict


def verifyConditions(x):
    """
    Funció booleana (retorna un boolea) rep un index
    i comprova si es verifiquen reggex
    """

    # mirem si esta en angles i despres mirem si té les paraules que busquem
    if isinstance(x,str) and re.search(r"mystery", x, re.IGNORECASE) and re.search(r"crime", x,re.IGNORECASE) :
        return True
    return False


def filterByReggex(df):
    """
    Filtrem un df per els verifyConditions
    :param df: df sobre el que filtrem
    :return: retorna el df filtrat
    """
    # mirem nomes si està en angles
    df = df.loc[df["original_language"]=="en"]
    # Ens quedem amb les que verifiquin el reggex
    df = df.loc[df["overview"].apply(verifyConditions)]
    return df


def filterByReggex32(df):
    """
    Filtrem per el any 2023 i que el statussigui cancelat.
    :param df: df sobre el que filtrem
    :return: retorna el df filtrat
    """
    # mirem si la data és 2023
    df = df.loc[df["first_air_date"].apply(lambda x : x.year == 2023)]

    # Filtrem les que tinguin "Canceled" en status
    # df = df.loc[df["status\r"]=="Canceled\r"]
    df = df.loc[df["status\r"].apply(lambda x : "Canceled" in str(x))]
    return df


def isRegJapLangCheck(x):
    """
    :param x: valor a verificar amb un reggex
    :return:
    """
    if isinstance(x, str):
        if re.search("ja", x, re.IGNORECASE):
            return True
    return False


def filterByReggexJapLang(df):
    """
    Filtrem buscant pelicules amb idioma original japonès
    :param df: df amb dades
    :return: df amb els camps requerits al exercici.
    """
    df["isGood"] = df["languages"].apply(isRegJapLangCheck)
    df = df[df["isGood"]]

    # Borrem les columnes incecesaries:
    df = df[["name", "original_name", "production_companies"]]
    return df


def funt_counter(df, search_val):
    """
    :param df: df amb les dades
    :param search_val: string amb el any que volem usar per filtrar
    :return: retorna el numero de files que verifiquen la condició de filtratge
    """
    df_copy = df
    df_copy = df_copy.loc[df["first_air_date"].apply(lambda x: x.year == int(search_val))]
    return int(df_copy.shape[0])