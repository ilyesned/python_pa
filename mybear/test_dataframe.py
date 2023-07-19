import pytest
from series import Series
from dataframe import DataFrame

def test_dataframe_max():
    series1 = Series([25, 32, 45], "Age")
    series2 = Series(["John", "Jane", "Michael"], "Prenom")
    series3 = Series(["Doe", "Smith", "Johnson"], "Nom")
    series_list = [series1, series2, series3]
    df = DataFrame(series_list)
    max_df = df.max()
    assert max_df.data["Age"] == [45]
    assert max_df.data["Prenom"] == ["Michael"]
    assert max_df.data["Nom"] == ["Smith"]

def test_dataframe_min():
    series1 = Series([25, 32, 45], "Age")
    series2 = Series(["John", "Jane", "Michael"], "Prenom")
    series3 = Series(["Doe", "Smith", "Johnson"], "Nom")
    series_list = [series1, series2, series3]
    df = DataFrame(series_list)
    min_df = df.min()
    assert min_df.data["Age"] == [25]
    assert min_df.data["Prenom"] == ["Jane"]
    assert min_df.data["Nom"] == ["Doe"]
def test_dataframe_mean():
    series1 = Series([25, 32, 45], "Age")
    series2 = Series(["John", "Jane", "Michael"], "Prenom")
    series3 = Series(["Doe", "Smith", "Johnson"], "Nom")
    series_list = [series1, series2, series3]
    df = DataFrame(series_list)
    mean_df = df.mean()
    assert mean_df.data.get("Age") is None
    # Les colonnes de type chaîne de caractères ne sont pas applicables pour le calcul de la moyenne, donc elles devraient être exclues

def test_dataframe_count():
    series1 = Series([25, 32, 45], "Age")
    series2 = Series(["John", "Jane", "Michael"], "Prenom")
    series3 = Series(["Doe", "Smith", "Johnson"], "Nom")
    series_list = [series1, series2, series3]
    df = DataFrame(series_list)
    count_df = df.count()
    assert count_df.data["Age"] == [3]
    assert count_df.data["Prenom"] == [3]
    assert count_df.data["Nom"] == [3]

def test_dataframe_std():
    series1 = Series([25, 32, 45], "Age")
    series2 = Series(["John", "Jane", "Michael"], "Prenom")
    series3 = Series(["Doe", "Smith", "Johnson"], "Nom")
    series_list = [series1, series2, series3]
    df = DataFrame(series_list)
    std_df = df.std()
    assert std_df.data.get("Age") is None
    # Les colonnes de type chaîne de caractères ne sont pas applicables pour le calcul de l'écart-type, donc elles devraient être exclues

def test_read_csv():
    df = DataFrame.read_csv("data.csv")
    assert list(df.data.keys()) == ["Prenom", "Nom", "Age"]
    assert df.data["Prenom"] == ["John", "Jane", "Michael", "Emily", "David", "Sarah", "Robert", "Olivia", "William"]

def test_read_json():
    df = DataFrame.read_json("data.json", orient="records")
    assert list(df.data.keys()) == ["Prenom", "Nom", "Age"]
    assert df.data["Prenom"] == ["John", "Jane", "Michael", "Emily", "David", "Sarah", "Robert", "Olivia", "William"]

def test_groupby():
    series1 = Series([25, 32, 45, 25, 32, 45], name="Age")
    series2 = Series(["John", "Jane", "Michael", "John", "Jane", "Michael"], name="Prenom")
    series3 = Series(["Doe", "Smith", "Johnson", "Doe", "Smith", "Johnson"], name="Nom")
    series_list = [series1, series2, series3]
    df = DataFrame(series_list)
    groupby_df = df.groupby("Prenom", agg={"Age": max, "Nom": min})
    assert groupby_df.data["Prenom"] == ["Jane", "John", "Michael"]
    assert groupby_df.data["Age"] == [32, 25, 45]
    assert groupby_df.data["Nom"] == ["Smith", "Doe", "Johnson"]



