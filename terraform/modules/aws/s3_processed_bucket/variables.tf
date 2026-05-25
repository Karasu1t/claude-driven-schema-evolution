variable "environment" {
  type = string
}

variable "project" {
  type = string
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}
