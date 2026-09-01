variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "eu-west-2" # London
}

variable "project_name" {
  description = "Short name used to prefix/tag all resources"
  type        = string
  default     = "wind-turbine-pipeline"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "db_name" {
  description = "PostgreSQL database name"
  type        = string
  default     = "wind_turbine_db"
}

variable "db_username" {
  description = "Master username for RDS PostgreSQL"
  type        = string
  default     = "postgres"
}

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t4g.micro" # cheapest ARM-based burstable instance, fine for dev/portfolio use
}

variable "db_allocated_storage_gb" {
  description = "Allocated storage for RDS in GB"
  type        = number
  default     = 20
}

variable "glue_worker_type" {
  description = "Glue worker type for PySpark jobs"
  type        = string
  default     = "G.1X" # smallest standard worker, fine for this dataset size
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers per job run"
  type        = number
  default     = 2
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC housing RDS and Glue connections"
  type        = string
  default     = "10.0.0.0/16"
}

variable "tags" {
  description = "Common tags applied to all resources"
  type        = map(string)
  default = {
    Project   = "wind-turbine-pipeline"
    ManagedBy = "terraform"
  }
}
