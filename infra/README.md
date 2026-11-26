# Benchmark VM

```sh
export VULTR_API_KEY=...
terraform init
terraform apply -var allowed_ssh_ip=$(curl -s https://api.ipify.org)

IP=$(terraform output -raw ip)
ssh root@$IP 'until [ -f /root/ready ]; do sleep 10; done'     # first boot takes ~3 min
rsync -az --exclude node_modules --exclude target --exclude .env --exclude .git ../ root@$IP:bench/
scp ../.env root@$IP:bench/.env && ssh root@$IP 'chmod 600 bench/.env'
ssh root@$IP 'docker exec pg createdb -U postgres indexer && docker exec -i pg psql -U postgres -d indexer < bench/common/schema.sql'

terraform destroy                                                  # ~$0.24/hour while it exists
```
