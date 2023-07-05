# imports


def union_datasets(df_list, df=None):
    if df == None:
        df = df_list[-1]
    else:
        df = df.unionByName(df_list[0], allowMissingColumns=True)
    
    df_list.pop()

    if df_list:
        return union_datasets(df_list, df)
    else:
        return df   