resource "aws_instance" "betfair_scraper" {
  ami = "ami-035efd31ab8835d8a"
  instance_type = "t3.small"
  subnet_id = "subnet-06d7068fc11f91393"
  vpc_security_group_ids = ["sg-014f471226ce25629"]
  tags = {
    Name = "Betfair Scraper"
  }
  user_data = file("${path.module}/user_data/betfair_user_data.sh")
}

resource "aws_instance" "omqb_scraper" {
  ami = "ami-035efd31ab8835d8a"
  instance_type = "t3.small"
  subnet_id = "subnet-06d7068fc11f91393"
  vpc_security_group_ids = ["sg-0bcc01cdbf8b1c58a"]
  tags = {
    Name = "OMQB Scraper"
  }
  user_data = file("${path.module}/user_data/omqb_user_data.sh")
}