# Nodes have no SSH key and no open SSH port: access is SSM Session Manager.
resource "aws_iam_role" "node" {
  name = "kafka-lab-apache-node"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "node" {
  name = "kafka-lab-apache-node"
  role = aws_iam_role.node.name
}
