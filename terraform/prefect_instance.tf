resource "aws_instance" "prefect_instance" {
  ami                         = "ami-07652eda1fbad7432" # ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-20240701
  instance_type               = "t3.medium"
  associate_public_ip_address = true
  availability_zone           = "eu-central-1a"
  key_name                    = aws_key_pair.dev_ssh_key.key_name
  vpc_security_group_ids      = [aws_security_group.main_sg.id]

  user_data = data.template_file.prefect_install.rendered

  root_block_device {
    volume_size = 50
  }

  tags = {
    Name = "Prefect"
  }
}

output "instance_public_ip" {
  description = "Public IP address of the Prefect EC2 instance"
  value       = aws_instance.prefect_instance.public_ip
}

output "instance_id" {
  description = "ID of the Prefect EC2 instance"
  value       = aws_instance.prefect_instance.id
}
