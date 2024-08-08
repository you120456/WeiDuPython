import pandas as pd
def save(text,path,**kwargs):
    writer = pd.ExcelWriter('{0}/{1}.xlsx'.format(path, text))
    for i,k in kwargs.items():
        k.to_excel(writer, sheet_name=i, index=False)
    writer.close()
