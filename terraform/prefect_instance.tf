resource "aws_instance" "prefect_instance" {
  ami                         = "ami-07652eda1fbad7432"  # ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-20240701
  instance_type               = "t3.medium"
  associate_public_ip_address = true
  availability_zone           = "eu-central-1a"
  key_name                    = var.aws_key_pair

#   user_data = data.template_file.prefect_install.rendered

  tags = {
    Name = "Prefect"
  }
}

resource "aws_key_pair" "ssh-key" {
  key_name   = "ssh-key"
  public_key = "ssh-rsa AAAAB3Nza............"
}

# Create a separate volume in case we ever need to destroy and recreate the instance. We need the data!
# In case you change the size of the EBS volume, you need to SSH into the EC2 instance, confirm extra block size (`lsblk`), then run `sudo resize2fs /dev/nvme1n1` to fill up the new space
resource "aws_ebs_volume" "prefect_storage" {
  availability_zone = "eu-central-1a"
  size              = 10
  type              = "gp3"

  tags = {
    Name = "Prefect storage"
  }
}

resource "aws_volume_attachment" "prefect_attachment" {
  volume_id   = aws_ebs_volume.prefect_storage.id
  instance_id = aws_instance.prefect_instance.id
  device_name = "/dev/sdh"
}

output "instance_public_ip" {
  description = "Public IP address of the Prefect EC2 instance"
  value       = aws_instance.prefect_instance.public_ip
}

output "instance_id" {
  description = "ID of the Prefect EC2 instance"
  value       = aws_instance.prefect_instance.id
}