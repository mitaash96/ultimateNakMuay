import pyspark.sql.functions as F
import pyspark.sql.types as T
import re


def roman_to_int(roman):
    roman_mapping = {
        'I': 1,
        'II': 2,
        'III': 3,
        'IV': 4,
        'V': 5,
        'VI': 6,
        'VII': 7,
        'VIII': 8,
        'IX': 9,
        'X': 10
    }
    pattern = r'\b(I{1,3}|IV|V|VI{0,3}|IX|X)\b'
    
    def replace(match):
        return str(roman_mapping[match.group(0)])
    
    return re.sub(pattern, replace, roman)


def event_name_correction(correction_list, df):
    for _ in correction_list:
        df = df.withColumn("event_name", F.regexp_replace(F.col("event_name"), _[0], _[1]))
    return df


def drop_join_cols(df):
    df = df.drop(*[col for col in df.columns if 'jc' in col])
    return df