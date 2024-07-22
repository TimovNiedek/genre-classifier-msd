# IaC setup

This document describes how to setup the environment using terraform.

## Prerequisites

You need to have generated an ssh key pair and have the private key available on your machine.
The name should be `dev_key` and `dev_key.pub` for the private and public key respectively. 
For instructions how to generate the ssh key, see [Generating Your SSH Public Key](https://git-scm.com/book/it/v2/Git-on-the-Server-Generating-Your-SSH-Public-Key).

### `variables.tfvars`

Create a file named `variables.tfvars` in the `terraform` directory with the following content:

```hcl
dev_ssh_public_key = "ssh-rsa AAAAB3NzaC1y..."  # your public key
```
