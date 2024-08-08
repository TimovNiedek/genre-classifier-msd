data "template_file" "prefect_install" {
  template = file("${path.module}/templates/install_prefect.tpl")
  vars = {
    prefect_service       = file("${path.module}/templates/prefect_server.service.tpl")
    work_pool_service     = file("${path.module}/templates/prefect_work_pool.service.tpl")
    mlflow_server_service = file("${path.module}/templates/mlflow_server.service.tpl")
  }
}
