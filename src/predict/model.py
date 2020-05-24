import xgboost as xgb
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.model_selection import train_test_split, GridSearchCV
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier

plt.rcParams['figure.figsize'] = 20, 10
from src.config import data_root


def plot_confusion_matrix(cm, classes, normalized=True, cmap='bone', title=""):
    norm_cm = cm
    if normalized:
        norm_cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        sns.heatmap(norm_cm, annot=cm, fmt='g', xticklabels=classes, yticklabels=classes, cmap=cmap)
        plt.title(f"{title}")
        plt.show()


def boost_cv(data: pd.DataFrame):
    X, y = data.iloc[:, :-1], data.iloc[:, -1]
    data_dmatrix = xgb.DMatrix(data=X, label=y)
    params = {
        "objective": "reg:squarederror",
        "colsample_bytree": 0.3,
        "learning_rate": 0.25,
        "max_depth": 40,
        "alpha": 50,
        "n_estimators": 100,
        "reg_lambda": 30,
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


def test_ticket_method(data: pd.DataFrame, method, method_name):
    X, y = data.iloc[:, :-1], data.iloc[:, -1]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    method.fit(X_train, y_train)
    preds = method.predict(X_test)
    preds = list(map(lambda x: round(x), preds))
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)
    print(f"[{method_name}] RMSE: {rmse}")
    print(f"[{method_name}] MAE: {mae}")
    print(f"[{method_name}] R2: {r2}")

    bundle = data.copy().iloc[y_test.index]
    bundle['PREDICTED'] = preds
    bundle['DIFF'] = bundle['DAYSINDEVELOPMENT'] - bundle['PREDICTED']
    bundle['DIFFPERCENT'] = abs(bundle['DIFF']) / (bundle['DAYSINDEVELOPMENT'] + 1)
    bundle = bundle.sort_values(by='DIFFPERCENT')
    bundle.to_csv(f'{data_root}/prediction_data/hot_encoded_model_data_development_{method_name}_predictions.csv',
                  index=False)

    return bundle


def test_dev_boost_method(data: pd.DataFrame):  # , method, method_name):
    X, y = data.iloc[:, :-1], data.iloc[:, -1]
    classes = y.unique()
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)
    d_train = xgb.DMatrix(data=X_train, label=y_train)
    d_test = xgb.DMatrix(data=X_test, label=y_test)

    model = xgb.train({
        "max_depth": 30,
        "objective": "multi:softmax",
        "num_class": len(classes) + 1,
        "learning_rate": 0.05,
        "n_gpus": 0
    }, d_train)
    preds_raw = model.predict(d_test)
    print(classification_report(y_test, preds_raw))
    cm = confusion_matrix(y_test, preds_raw)

    return preds_raw, cm


def test_dev_method(data, method, method_name):
    X, y = data.iloc[:, :-1], data.iloc[:, -1]
    classes = y.unique()
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    model = method.fit(X_train, y_train)
    accuracy = model.score(X_test, y_test)
    print(f"[{method_name}] Accuracy: {accuracy}")
    preds = model.predict(X_test)
    cm = confusion_matrix(y_test, preds)
    plot_confusion_matrix(cm, classes, title=f"{method_name}")
    return accuracy, cm


if __name__ == '__main__':
    # # ticket_fname = f'{data_root}/prediction_data/ticket_model/encoded_model_data_development_summed.csv'
    # # ticket_fname = f'{data_root}/prediction_data/ticket_model/encoded_model_data_development_filtered.csv'
    # # ticket_fname = f'{data_root}/prediction_data/ticket_model/hot_encoded_model_data.csv'
    # ticket_model_data = pd.read_csv(ticket_fname)
    # # boosted_generic = boost(ticket_model_data)
    # boost_cv(ticket_model_data)
    # boosted = test_ticket_method(ticket_model_data, xgb.XGBRegressor(
    #     objective='reg:squarederror',
    #     colsample_bytree=0.3,
    #     learning_rate=0.25,
    #     max_depth=40,
    #     alpha=50,
    #     n_estimators=100,
    #     reg_lambda=30,
    # ), 'boost')
    # # boosted[abs(boosted['DIFF']) < 2].shape[0] / boosted.shape[0]
    # naive = test_ticket_method(ticket_model_data, GaussianNB(), 'naive')
    # forest = test_ticket_method(ticket_model_data, RandomForestClassifier(max_depth=50, random_state=42), 'forest')

    dev_fname = f'{data_root}/prediction_data/dev_model/encoded_model_data_unfiltered.csv'
    dev_model_data = pd.read_csv(dev_fname)
    dev_boosted, boosted_cm = test_dev_boost_method(dev_model_data)
    forest_acc, forest_cm = test_dev_method(dev_model_data, RandomForestClassifier(max_depth=50, random_state=42), 'RandomForest')
    svm_acc, svm_cm = test_dev_method(dev_model_data, SVC(kernel='rbf', gamma=0.1, C=1000), 'SVM')
    knn_acc, knn_cm = test_dev_method(dev_model_data, KNeighborsClassifier(n_neighbors=10), 'kNN')
    naive_acc, naivi_cm = test_dev_method(dev_model_data, GaussianNB(), 'naive')
