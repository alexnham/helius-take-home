# The benchmark VM from docs/benchmarks.md: Vultr vc2-8c-32gb in Amsterdam, next to the Old Faithful mirror.
# Auth: export VULTR_API_KEY. SSH is open only to allowed_ssh_ip.

terraform {
  required_providers {
    vultr = {
      source  = "vultr/vultr"
      version = "~> 2.33"
    }
  }
}

provider "vultr" {}

variable "allowed_ssh_ip" {
  description = "Your public IPv4 address; the only source allowed to SSH in"
  type        = string
}

variable "ssh_public_key_path" {
  type    = string
  default = "~/.ssh/id_rsa.pub"
}

resource "vultr_ssh_key" "bench" {
  name    = "helius-bench"
  ssh_key = trimspace(file(pathexpand(var.ssh_public_key_path)))
}

resource "vultr_firewall_group" "bench" {
  description = "helius-bench"
}

resource "vultr_firewall_rule" "ssh" {
  firewall_group_id = vultr_firewall_group.bench.id
  protocol          = "tcp"
  ip_type           = "v4"
  subnet            = var.allowed_ssh_ip
  subnet_size       = 32
  port              = "22"
}

resource "vultr_instance" "bench" {
  label             = "helius-bench"
  hostname          = "helius-bench"
  region            = "ams"
  plan              = "vc2-8c-32gb" # 8 vCPU, 32 GB, 640 GB SSD
  os_id             = 2284          # Ubuntu 24.04 LTS x64
  ssh_key_ids       = [vultr_ssh_key.bench.id]
  firewall_group_id = vultr_firewall_group.bench.id
  backups           = "disabled"
  user_data         = file("${path.module}/cloud-init.sh")
}

output "ip" {
  value = vultr_instance.bench.main_ip
}
