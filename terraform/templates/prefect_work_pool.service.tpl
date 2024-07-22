[Unit]
Description=Prefect Server

[Service]
Restart=always
RestartSec=10
ExecStart=prefect worker start --pool "docker-work-pool"

[Install]
WantedBy=multi-user.target
