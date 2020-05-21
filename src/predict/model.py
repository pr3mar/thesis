import xgboost as xgb
from sklearn.metrics import mean_squared_error
from sklearn.naive_bayes import GaussianNB
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier

plt.rcParams['figure.figsize'] = 20, 10
from src.config import data_root


def boost_cv(data: pd.DataFrame):
    X, y = data.iloc[:, :-1], data.iloc[:, -1]
    data_dmatrix = xgb.DMatrix(data=X, label=y)
    params = {
        "objective": "reg:squarederror",
        'colsample_bytree': 0.4,
        'learning_rate': 0.15,
        'max_depth': 30,
        'alpha': 30
    }
    cv_results = xgb.cv(
        dtrain=data_dmatrix,
        params=params,
        nfold=10,
        num_boost_round=150,
        early_stopping_rounds=10,
        metrics="rmse",
        as_pandas=True,
        seed=42
    )
    print('finally: ', (cv_results["test-rmse-mean"]).tail(1))


def boost(data: pd.DataFrame):
    X, y = data.iloc[:, :-1], data.iloc[:, -1]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.05, random_state=42)

    xg_reg = xgb.XGBRegressor(
        objective='reg:squarederror',
        colsample_bytree=0.3,
        learning_rate=0.25,
        max_depth=40,
        alpha=50,
        n_estimators=100,
        reg_lambda=30,
    )
    xg_reg.fit(X_train, y_train)
    preds = xg_reg.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    print(f"[boost] RMSE: {rmse}")

    xgb.plot_tree(xg_reg, num_trees=0)
    plt.savefig(f"{data_root}/figs/tree_development.svg", format="svg")
    xgb.plot_importance(xg_reg)
    plt.rcParams['figure.figsize'] = [5, 5]
    plt.savefig(f"{data_root}/figs/importance_development.svg", format="svg")

    bundle = data.copy().iloc[y_test.index]
    bundle['TARGET'] = preds
    bundle['DIFF'] = abs(bundle['TARGET'] - bundle['DAYSINDEVELOPMENT'])
    return bundle.sort_values(by='DIFF')


def test_method(data: pd.DataFrame, method, method_name):
    X, y = data.iloc[:, :-1], data.iloc[:, -1]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    method.fit(X_train, y_train)
    preds = method.predict(X_test)
    preds = list(map(lambda x: round(x), preds))
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    print(f"[{method_name}] RMSE: {rmse}")

    bundle = data.copy().iloc[y_test.index]
    bundle['TARGET'] = preds
    bundle['DIFF'] = bundle['DAYSINDEVELOPMENT'] - bundle['TARGET']
    bundle = bundle.sort_values(by='DIFF')
    bundle.to_csv(f'{data_root}/prediction_data/hot_encoded_model_data_development_{method_name}_predictions.csv', index=False)

    return bundle


if __name__ == '__main__':
    fname = f'{data_root}/prediction_data/hot_encoded_model_data_development.csv'
    # fname = f'{data_root}/prediction_data/hot_encoded_model_data.csv'
    model_data = pd.read_csv(fname)
    boosted_generic = boost(model_data)
    boosted = test_method(model_data, xgb.XGBRegressor(
        objective='reg:squarederror',
        colsample_bytree=0.3,
        learning_rate=0.25,
        max_depth=40,
        alpha=50,
        n_estimators=100,
        reg_lambda=30,
    ), 'boost')
    # boosted[abs(boosted['DIFF']) < 2].shape[0] / boosted.shape[0]
    naive = test_method(model_data, GaussianNB(), 'naive')
    forest = test_method(model_data, RandomForestClassifier(max_depth=50, random_state=42), 'forest')
