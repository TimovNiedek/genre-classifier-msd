[Unit]
Description=MLflow server

[Service]
Restart=always
RestartSec=10
ExecStart=mlflow server -h 0.0.0.0 -p 5000 --backend-store-uri sqlite:///mlflow.db --default-artifact-root s3://mlflow-artifacts-tvn

[Install]
WantedBy=multi-user.target
