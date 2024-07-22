data "template_file" "prefect_install" {
  template = file("${path.module}/templates/install_prefect.tpl")
}