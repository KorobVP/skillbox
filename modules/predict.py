from datetime import datetime

import dill
import pandas as pd
import os
import json


# project_path = 'd:/Python/SB/HW33/airflow_hw/'
# project_path = '~/airflow_hw/'
project_path = os.path.expanduser('~/airflow_hw/')


# ищем в папке path модель с максимальным временем создания
def last_model(path):
    res = ''
    for filename in os.listdir(project_path + 'data/models/'):
        if filename > res:
            res = filename
    return res


# делаем прогноз для одного файла file при помощи модели model
def prediction(file, model):
    with open(file, 'r') as f:
        text = f.read()
    df = pd.DataFrame(json.loads(text), index=[0])
    pred = model.predict(df)
    df['result'] = pred
    return df


# делаем прогнозы для всех файлов в папке test, данные и результаты сохраняем в файл
def predict():
    result = pd.DataFrame()
    model_path = project_path + 'data/models/' + last_model(project_path + 'data/models/')
    with open(model_path, 'rb') as f:
        model = dill.load(f)

    for filename in os.listdir(project_path + 'data/test/'):
        result = pd.concat([result, prediction(project_path + 'data/test/' + filename, model)])

    result.reset_index(inplace=True)
    result.drop(['index'], axis=1, inplace=True)
    result.to_csv(project_path + f'data/predictions/preds{datetime.now().strftime("%Y%m%d%H%M")}.csv')


if __name__ == '__main__':
    predict()

