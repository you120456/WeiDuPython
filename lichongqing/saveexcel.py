import pandas as pd
import os
def save(text,path,**kwargs):
    if not os.path.exists(path):
        os.makedirs(path)
    writer = pd.ExcelWriter('{0}/{1}.xlsx'.format(path, text))
    for i,k in kwargs.items():
        k.to_excel(writer, sheet_name=i, index=False)
    writer.close()
