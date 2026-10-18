terraform {
  required_version = ">= 1.10.0"

  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = "~> 2.0"
    }
  }

  backend "s3" {
    bucket                      = "digital-ocean-test-tfstate-821117"
    key                         = "job-api/terraform.tfstate"
    region                      = "us-east-1"
    endpoints                   = { s3 = "https://nyc3.digitaloceanspaces.com" }
    skip_credentials_validation = true
    skip_region_validation      = true
    skip_metadata_api_check     = true
    skip_requesting_account_id  = true
    use_path_style              = true
    skip_s3_checksum            = true
  }
}

provider "digitalocean" {
  token = var.digitalocean_token
}

resource "digitalocean_database_cluster" "jobs" {
  name       = var.database_cluster_name
  engine     = "pg"
  version    = "16"
  size       = var.database_size
  region     = var.database_region
  node_count = 1

  lifecycle {
    prevent_destroy = true
  }
}

resource "digitalocean_database_cluster" "queue" {
  name       = var.queue_cluster_name
  engine     = "valkey"
  version    = "8"
  size       = var.queue_size
  region     = var.database_region
  node_count = 1

  lifecycle {
    prevent_destroy = true
  }
}

resource "digitalocean_project" "jobs" {
  name        = var.project_name
  description = "Resources for the asynchronous job service."
  purpose     = "Service or API"
  environment = "Development"
  resources = [
    digitalocean_database_cluster.jobs.urn,
    digitalocean_database_cluster.queue.urn,
    "do:app:${digitalocean_app.jobs.id}",
  ]
}

resource "digitalocean_database_db" "jobs" {
  cluster_id = digitalocean_database_cluster.jobs.id
  name       = "jobs"
}

resource "digitalocean_database_firewall" "jobs" {
  cluster_id = digitalocean_database_cluster.jobs.id

  rule {
    type  = "app"
    value = digitalocean_app.jobs.id
  }
}

resource "digitalocean_database_firewall" "queue" {
  cluster_id = digitalocean_database_cluster.queue.id

  rule {
    type  = "app"
    value = digitalocean_app.jobs.id
  }
}

resource "digitalocean_app" "jobs" {
  spec {
    name   = var.app_name
    region = var.app_region

    service {
      name               = "api"
      instance_count     = 1
      instance_size_slug = var.app_instance_size
      source_dir         = "/"
      dockerfile_path    = "Dockerfile"
      http_port          = 3000

      github {
        repo           = var.github_repo
        branch         = var.github_branch
        deploy_on_push = true
      }

      health_check {
        http_path             = "/health/ready"
        initial_delay_seconds = 30
        period_seconds        = 10
        timeout_seconds       = 5
        success_threshold     = 1
        failure_threshold     = 3
      }

      env {
        key   = "DATABASE_URL"
        value = "$${jobs-db.DATABASE_URL}"
        type  = "SECRET"
        scope = "RUN_TIME"
      }

      env {
        key   = "REDIS_URL"
        value = digitalocean_database_cluster.queue.uri
        type  = "SECRET"
        scope = "RUN_TIME"
      }

      env {
        key   = "NODE_ENV"
        value = "production"
        type  = "GENERAL"
        scope = "RUN_TIME"
      }
    }

    database {
      name         = "jobs-db"
      engine       = "PG"
      version      = "16"
      production   = true
      cluster_name = digitalocean_database_cluster.jobs.name
      db_name      = digitalocean_database_db.jobs.name
      db_user      = digitalocean_database_cluster.jobs.user
    }

    # Valkey is connected through the REDIS_URL secret above, not an App Platform database binding.
  }
}
