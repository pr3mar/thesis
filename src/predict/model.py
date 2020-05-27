import xgboost as xgb
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.svm import SVC, SVR
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
    bundle['DIFF'] = abs(bundle['TARGET'] - bundle['HOURSINDEVELOPMENT'])
    return bundle.sort_values(by='DIFF')


def test_ticket_method(data: pd.DataFrame, method, method_name, target_feature):
    X, y = data.iloc[:, :-1], data.iloc[:, -1]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    method.fit(X_train, y_train)
    preds = method.predict(X_test)
    preds = list(map(lambda x: round(x), preds))
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)
    # print(f"[{method_name}] RMSE: {rmse}")
    # print(f"[{method_name}] MAE: {mae}")
    # print(f"[{method_name}] R2: {r2}")
    return rmse, mae, r2
    # leftover from analysis of the predictions
    # bundle = data.copy().iloc[y_test.index]
    # bundle['PREDICTED'] = preds
    # bundle['DIFF'] = bundle[target_feature] - bundle['PREDICTED']
    # bundle['DIFFPERCENT'] = abs(bundle['DIFF']) / (bundle[target_feature] + 1)
    # bundle = bundle.sort_values(by='DIFFPERCENT')
    # bundle.to_csv(f'{data_root}/prediction_data/hot_encoded_model_data_development_{method_name}_predictions.csv', index=False)
    # return bundle


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
    # ticket_fname = f'{data_root}/prediction_data/ticket_model/encoded_model_data_development_summed.csv'
    base_fname = f'{data_root}/prediction_data/ticket_model'
    methods = [
        (xgb.XGBRegressor(
            objective='reg:squarederror',
            colsample_bytree=0.3,
            learning_rate=0.25,
            max_depth=40,
            alpha=50,
            n_estimators=100,
            reg_lambda=30,
        ), "boost"),
        (GaussianNB(), "naive"),
        (RandomForestClassifier(max_depth=50, random_state=42), 'forest'),
        (SVR(C=1.0, epsilon=0.2), 'SVM')
    ]
    atts = [
        ('days', 'encoded_model_data_development_hours.csv', "HOURSINDEVELOPMENT"),  # _2 => min value is 2
        ('days', 'encoded_model_data_development_hours_real-data.csv', "HOURSINDEVELOPMENT"),  # _2 => min value is 2
        # ('days', 'encoded_model_data_development_filtered_2.csv', "DAYSINDEVELOPMENT"),  # _2 => min value is 2
        # # ('days', 'encoded_model_data_development_filtered.csv', "DAYSINDEVELOPMENT"),
        # ('days-real', 'encoded_model_data_development_filtered_2_real-data.csv', "DAYSINDEVELOPMENT"),
        # # ('days-real', 'encoded_model_data_development_filtered_real-data.csv', "DAYSINDEVELOPMENT"),
        # # ('hours', 'encoded_model_data_development_filtered_hours.csv', "HOURSINDEVELOPMENT"),
        # ('hours', 'encoded_model_data_development_filtered_hours_2.csv', "HOURSINDEVELOPMENT"),
        # # ('hours-real', 'encoded_model_data_development_filtered_hours_real-data.csv', "HOURSINDEVELOPMENT"),
        # ('hours-real', 'encoded_model_data_development_filtered_hours_2_real-data.csv', "HOURSINDEVELOPMENT"),
        # ('hours-filtered-30', 'encoded_model_data_development_filtered_hours_30-days.csv', "HOURSINDEVELOPMENT"),
        # ('hours-filtered-30-real', 'encoded_model_data_development_filtered_hours_30-days_real-data.csv', "HOURSINDEVELOPMENT"),
        # ('hours-filtered-10', 'encoded_model_data_development_filtered_hours_10-days.csv', "HOURSINDEVELOPMENT"),
        # ('hours-filtered-10-real', 'encoded_model_data_development_filtered_hours_10-days_real-data.csv', "HOURSINDEVELOPMENT"),
    ]
    print("RMSE & MAE & R2 \\\\")
    data_descriptions = {}
    for data_type, ticket_fname, target_feature in atts:
        print(data_type)
        fname = f'{base_fname}/{ticket_fname}'
        data = pd.read_csv(fname)
        data_descriptions[data_type] = data[target_feature].describe()
        # boosted_generic = boost(data)
        # boost_cv(data)
        for method, name in methods:
            rmse, mae, r2 = test_ticket_method(
                data=data,
                method=method,
                method_name=name,
                target_feature=target_feature
            )
            print(f"& {name} & {rmse:.3f} & {mae:.3f} & {r2:.3f} \\\\")
        print("\\hline")
    description = pd.DataFrame(data_descriptions)
    # dev_fname = f'{data_root}/prediction_data/dev_model/encoded_model_data_unfiltered.csv'
    # dev_model_data = pd.read_csv(dev_fname)
    # dev_boosted, boosted_cm = test_dev_boost_method(dev_model_data)
    # forest_acc, forest_cm = test_dev_method(dev_model_data, RandomForestClassifier(max_depth=50, random_state=42), 'RandomForest')
    # svm_acc, svm_cm = test_dev_method(dev_model_data, SVC(kernel='rbf', gamma=0.1, C=1000), 'SVM')
    # knn_acc, knn_cm = test_dev_method(dev_model_data, KNeighborsClassifier(n_neighbors=10), 'kNN')
    # naive_acc, naivi_cm = test_dev_method(dev_model_data, GaussianNB(), 'naive')
