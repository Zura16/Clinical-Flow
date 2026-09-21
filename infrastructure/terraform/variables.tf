variable "resource_group_name" {
  default = "rg-clinicalflow-prod-eastus"
}

variable "location" {
  default = "eastus"
}

variable "storage_account_name" {
  default = "stclinicalflowadls2026"
}

variable "sql_server_name" {
  default = "sql-clinicalflow-server-2026"
}

variable "admin_username" {
  default = "sqladmin"
}

variable "admin_password" {
  description = "SQL admin password. No default: supply via TF_VAR_admin_password or a gitignored *.tfvars file."
  type        = string
  sensitive   = true
}

variable "adf_name" {
  default = "adf-clinicalflow-2026"
}

variable "databricks_workspace_name" {
  default = "dbw-clinicalflow-2026"
}
