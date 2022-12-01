import os

print('Inside do Training')

import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import AdaBoostClassifier
import joblib
from mlflow.tracking import MlflowClient
from urllib.parse import parse_qsl, urljoin, urlparse
import mlflow
import mlflow.sklearn
import logging

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

from pathlib import Path
import sqlite3

Parent = Path(__file__).resolve().parents[5]
print(Parent)
os.chdir(Parent)
database_path = str(Parent)+"/Sqlite/mlruns.db"
mlflow.set_tracking_uri("sqlite:///"+database_path)
#process_url = str(Parent)+'/Sqlite/DataOps.db'
print(database_path)
# Create your connection.
cnx = sqlite3.connect(database_path)

def eval_metrics(lift_df):
    lift_df["Rank"] = np.arange(lift_df.shape[0])
    lift_df["Quantile"] = pd.qcut(lift_df["Rank"], q=9,labels=['Q1','Q2','Q3','Q4','Q5',
                                          'Q6','Q7','Q8','Q9'])

    coverage_df = lift_df.groupby("Quantile").agg({'Quantile':'count', 'Prob_for_Error':'max','Prob_for_Error':['max','min'],
                                    'Actuals': 'sum'})
    coverage_df.columns = ['Quantile_count','Prediction_probablity_max',
                          'Prediction_probablity_min','Errors_Coverage']
    coverage_df['Error_cum_percent'] = (coverage_df.Errors_Coverage.cumsum() / coverage_df.Errors_Coverage.sum()) * 100
    return coverage_df,coverage_df['Error_cum_percent'][0],coverage_df['Error_cum_percent'][1],coverage_df['Error_cum_percent'][2]


def do_training(X_train, X_test, y_train, y_test,i):
    mlflow.set_experiment(experiment_name=i)
    experiment = mlflow.get_experiment_by_name(i)
    print("Name: {}".format(experiment.name))
    print("Experiment_id: {}".format(experiment.experiment_id))
    print("Artifact Location: {}".format(experiment.artifact_location))
    print("Tags: {}".format(experiment.tags))
    print("Lifecycle_stage: {}".format(experiment.lifecycle_stage))
    clf1 = LogisticRegression(multi_class='multinomial', random_state=1)
    clf2 = RandomForestClassifier(n_estimators=1000,verbose=1, max_depth=100)
    clf3 = GradientBoostingClassifier(n_estimators=1000, learning_rate=1.0,
        max_depth=1, random_state=0).fit(X_train, y_train)
    clf4 = MLPClassifier(solver='lbfgs', alpha=1e-5,
                    hidden_layer_sizes=(5,), random_state=1)
    cl5 = AdaBoostClassifier(n_estimators=1000)
    eclf = VotingClassifier(estimators=[
        ('rf', clf2),('gbc',clf3),('mlp',clf4)], voting='soft')
    print(eclf.get_params(deep=False))
    with mlflow.start_run(run_name='TEST') as run:
        run_id = run.info.run_id
        eclf.fit(X_train, y_train)
        y_pred = eclf.predict_proba(X_test)[:, 1]
        y_pred_t = eclf.predict_proba(X_train)[:, 1]
        lift_df = pd.DataFrame(y_pred,y_test['Error']).reset_index()
        lift_df.columns = ['Actuals','Prob_for_Error']
        lift_df.sort_values('Prob_for_Error',ascending=False,inplace=True)
        lift_df.reset_index(drop=True,inplace=True)
        #lift_df.to_csv('./Buffer_Files/lift.csv')

        mlflow.log_param("estimators",eclf.get_params(deep=False))

        (coverage_df, Q1_Coverage, Q2_Coverage, Q3_Coverage) = eval_metrics(lift_df)
        print("  Q1_Coverage: %s" % Q1_Coverage)
        print("  Q2_Coverage: %s" % Q2_Coverage)
        print("  Q3_Coverage: %s" % Q3_Coverage)

        #mlflow.log_param("alpha", alpha)
        #mlflow.log_param("l1_ratio", l1_ratio)
        mlflow.log_metric("Q1_Coverage", Q1_Coverage)
        mlflow.log_metric("Q2_Coverage", Q2_Coverage)
        mlflow.log_metric("Q3_Coverage", Q3_Coverage)

        tracking_url_type_store = urlparse(mlflow.get_tracking_uri()).scheme
        print(tracking_url_type_store)
        print(mlflow.get_tracking_uri())
        reg_model_name = i

        art_loc = str(experiment.artifact_location)+'/'+str(run_id)+"/artifacts/"+i
        print(art_loc)
        X_train['Error'] = y_train.values
        # Model registry does not work with file store
        if tracking_url_type_store != "file":
            # Register the model
            # There are other ways to use the Model Registry, which depends on the use case,
            # please refer to the doc for more information:
            # https://mlflow.org/docs/latest/model-registry.html#api-workflow
            #mlflow.log_artifacts(local_dir='./mlruns')
            mlflow.sklearn.log_model(eclf, i, registered_model_name=reg_model_name)
            X_train.to_csv('./Map_Files/Train.csv', index=False)
            mlflow.log_artifact('./Map_Files/Train.csv', "Train_Data")

            ## Log CSV to MLflow

        else:
            #mlflow.log_artifact('./'),
            print("--")
            #mlflow.sklearn.log_model(eclf, i)
            #mlflow.log_artifacts(local_dir='./mlruns')
            X_train.to_csv('./Map_Files/Train.csv', index=False)
            mlflow.log_artifact('./Map_Files/Train.csv', "Train_Data")
            mlflow.sklearn.log_model(eclf, "sk_learn",
                                     serialization_format="cloudpickle",
                                     registered_model_name=reg_model_name)



    #run_id = run.info.run_id
    print("run_id: {}; lifecycle_stage: {}".format(run_id,mlflow.get_run(run_id).info.lifecycle_stage))
    client = MlflowClient()
    version_to_staging = client.get_latest_versions(name = i,stages = ["None"])[-1].version
    client.transition_model_version_stage(
        name=i,
        version=version_to_staging,
        stage="Staging"
    )

    return coverage_df