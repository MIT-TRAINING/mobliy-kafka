# One id for the whole cluster: 16 random bytes, unpadded base64url = 22 chars, the format kafka-storage expects
resource "random_id" "cluster" {
  byte_length = 16
}

resource "random_password" "admin" {
  length  = 24
  special = false
}

resource "random_password" "broker" {
  length  = 24
  special = false
}

resource "random_password" "trainer" {
  length  = 24
  special = false
}

resource "random_password" "learner" {
  for_each = toset(var.learners)
  length   = 16
  special  = false
}

locals {
  secrets_dir = "${path.module}/../secrets"
  user_passwords = merge(
    { for k, p in random_password.learner : k => p.result },
    { trainer = random_password.trainer.result },
  )
}

# Everything below is git-ignored (infra/cluster/secrets/)
resource "local_sensitive_file" "admin_properties" {
  filename        = "${local.secrets_dir}/admin.properties"
  file_permission = "0600"
  content         = <<-EOT
    bootstrap.servers=apache-kafka.${var.internal_zone}:9092
    security.protocol=SASL_PLAINTEXT
    sasl.mechanism=SCRAM-SHA-512
    sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="${random_password.admin.result}";
  EOT
}

# input for scripts/bootstrap-security.sh: user,password
resource "local_sensitive_file" "users_csv" {
  filename        = "${local.secrets_dir}/users.csv"
  file_permission = "0600"
  content         = "${join("\n", [for u, p in local.user_passwords : "${u},${p}"])}\n"
}

# tracking sheet for the trainer (learner rows are what goes into ~/kafka/apache.properties on each VM)
resource "local_sensitive_file" "credentials_csv" {
  filename        = "${local.secrets_dir}/cluster-credentials.csv"
  file_permission = "0600"
  content = join("\n", concat(
    ["id,kafka_user,kafka_password,bootstrap_servers,client_config_path"],
    [for u, p in local.user_passwords : "${u},${u},${p},apache-kafka.${var.internal_zone}:9092,~/kafka/apache.properties"],
    ["cluster-admin,admin,${random_password.admin.result},apache-kafka.${var.internal_zone}:9092,"],
  ))
}
