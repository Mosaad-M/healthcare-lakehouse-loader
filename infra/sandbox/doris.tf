# Doris on one EC2 instance (1 FE + 1 BE), private, reached as doris.lakehouse.internal.
# Administered through SSM only (no SSH, no public address). Production: 3 FE + 3 BE.
data "aws_ssm_parameter" "al2023_arm" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

resource "aws_instance" "doris" {
  ami                    = data.aws_ssm_parameter.al2023_arm.value
  instance_type          = "t4g.large"
  subnet_id              = aws_subnet.private[0].id
  vpc_security_group_ids = [aws_security_group.internal.id]
  iam_instance_profile   = aws_iam_instance_profile.doris.name
  metadata_options {
    http_tokens = "required"
  }
  root_block_device {
    volume_size = 60
    volume_type = "gp3"
    encrypted   = true
  }
  user_data  = <<-SH
    #!/bin/bash
    set -euxo pipefail
    dnf install -y docker
    systemctl enable --now docker
    sysctl -w vm.max_map_count=2000000
    # The instance can boot before the NAT route works: retry the image pulls until they succeed.
    for image in fe-4.1.4.1 be-4.1.4.1; do
      until docker pull "apache/doris:$image"; do sleep 20; done
    done
    TOKEN=$(curl -sX PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
    IP=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)
    docker run -d --name fe --network host --restart unless-stopped -e FE_SERVERS="fe1:$IP:9010" -e FE_ID=1 \
      --entrypoint bash apache/doris:fe-4.1.4.1 -c \
      "sed -i 's/-Xmx8192m -Xms8192m/-XX:-UseContainerSupport -Xmx2048m -Xms1024m/' /opt/apache-doris/fe/conf/fe.conf && exec bash init_fe.sh"
    docker run -d --name be --network host --restart unless-stopped -e FE_SERVERS="fe1:$IP:9010" -e BE_ADDR="$IP:9050" \
      --entrypoint bash apache/doris:be-4.1.4.1 -c \
      "echo 'mem_limit=3G' >> /opt/apache-doris/be/conf/be.conf && echo 'sys_log_level=WARNING' >> /opt/apache-doris/be/conf/be.conf && exec bash entry_point.sh"
  SH
  tags       = { Name = "${var.name}-doris" }
  depends_on = [aws_nat_gateway.main, aws_route_table_association.private]
  # The retrying script applies to new instances; do not replace a running one for it.
  lifecycle { ignore_changes = [user_data] }
}

# The namespace's hosted zone is managed by Cloud Map, so Doris is registered there as an instance.
resource "aws_service_discovery_service" "doris" {
  name = "doris"
  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id
    dns_records {
      type = "A"
      ttl  = 30
    }
    routing_policy = "MULTIVALUE"
  }
  force_destroy = true
}

resource "aws_service_discovery_instance" "doris" {
  instance_id = "doris"
  service_id  = aws_service_discovery_service.doris.id
  attributes = {
    AWS_INSTANCE_IPV4 = aws_instance.doris.private_ip
  }
}
