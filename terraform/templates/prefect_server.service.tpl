[Unit]
Description=Prefect Server

[Service]
Restart=always
RestartSec=10
ExecStart=prefect server start

[Install]
WantedBy=multi-user.target
