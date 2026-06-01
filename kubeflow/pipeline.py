import kfp
from kfp import dsl
from kfp.dsl import component, Input, Output, Dataset, Model

@component(
    base_image="python:3.9",
    packages_to_install=["pandas", "psycopg2-binary", "scikit-learn", "mlflow"]
)
def extract_from_postgres(output_path: Output[Dataset]):
    import pandas as pd
    import psycopg2
    conn = psycopg2.connect(
        host="postgres", database="orion", user="admin", password="admin123"
    )
    df = pd.read_sql("SELECT temperature, vibration, rpm, is_anomaly FROM features", conn)
    df.to_csv(output_path.path, index=False)

@component(
    base_image="python:3.9",
    packages_to_install=["pandas", "scikit-learn", "mlflow"]
)
def train_model(data_path: Input[Dataset], model_output: Output[Model]):
    import pandas as pd
    from sklearn.ensemble import IsolationForest
    import mlflow
    import mlflow.sklearn

    df = pd.read_csv(data_path.path)
    X = df[["temperature", "vibration", "rpm"]]
    model = IsolationForest(contamination=0.05)
    model.fit(X)

    mlflow.set_tracking_uri("http://mlflow:5000")
    with mlflow.start_run():
        mlflow.sklearn.log_model(model, "model")
        model_uri = mlflow.get_artifact_uri("model")
        mlflow.register_model(model_uri, "OrionAnomalyDetector")
    
    # Save model to output
    import joblib
    joblib.dump(model, model_output.path)

@dsl.pipeline(
    name="Orion Valley Training Pipeline",
    description="Retrains anomaly detection model daily"
)
def training_pipeline():
    extract_task = extract_from_postgres()
    train_task = train_model(data_path=extract_task.outputs["output"])
