variable "digitalocean_token" {
  description = "DigitalOcean API token. Supply through TF_VAR_digitalocean_token."
  type        = string
  sensitive   = true
}

variable "database_cluster_name" {
  description = "Name of the managed PostgreSQL cluster."
  type        = string
  default     = "digital-ocean-test-db"
}

variable "database_region" {
  description = "DigitalOcean database cluster region."
  type        = string
  default     = "nyc3"
}

variable "project_name" {
  description = "DigitalOcean project that groups the job API and database."
  type        = string
  default     = "Digital Ocean Test"
}

variable "database_size" {
  description = "Managed PostgreSQL node size."
  type        = string
  default     = "db-s-1vcpu-1gb"
}

variable "queue_cluster_name" {
  description = "Name of the managed Valkey queue cluster."
  type        = string
  default     = "digital-ocean-test-queue"
}

variable "queue_size" {
  description = "Managed Valkey node size."
  type        = string
  default     = "db-s-1vcpu-1gb"
}

variable "app_name" {
  description = "Unique name for the DigitalOcean App Platform application."
  type        = string
  default     = "digital-ocean-test"
}

variable "app_region" {
  description = "DigitalOcean App Platform region slug."
  type        = string
  default     = "nyc"
}

variable "app_instance_size" {
  description = "App Platform service instance size."
  type        = string
  default     = "apps-s-1vcpu-0.5gb"
}

variable "github_repo" {
  description = "GitHub repository connected to DigitalOcean App Platform."
  type        = string
  default     = "gauravdelhi1991/digital-ocean-test"
}

variable "github_branch" {
  description = "Branch deployed by DigitalOcean App Platform."
  type        = string
  default     = "main"
}
