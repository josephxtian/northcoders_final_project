resource "aws_scheduler_schedule" "lambda_ingestion_schedule" {
  name       = "nc-project-schedule"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(5 minutes)"

  target {
    arn      = aws_lambda_function.lambda_raw_data_to_ingestion_bucket.arn
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn
  }
}


# module "eventbridge_bus" {
#   source  = "terraform-aws-modules/eventbridge/aws"
#   bus_name = "my-bus"
# }

# module "eventbridge_rules" {
#   source  = "terraform-aws-modules/eventbridge/aws"
  
#   bus_name = module.eventbridge_bus.bus_name
#   create_bus = false  # Bus is created by `eventbridge_bus`

#   rules = {
#     logs = {
#       description   = "Capture log data"
#       event_pattern = jsonencode({ "source" : ["my.app.logs"] })
#     }
#     crons = {
#       description         = "Trigger for a Lambda"
#       schedule_expression = "rate(15 minutes)"
#     }
#   }
# }

# module "eventbridge_targets" {
#   source  = "terraform-aws-modules/eventbridge/aws"
  
#   bus_name = module.eventbridge_bus.bus_name
#   create_bus = false  

#   targets = {
#     logs = [
#       {
#         name = "send-logs-to-cloudwatch"
#         arn  = aws_cloudwatch_log_group.lambda_log.arn 
#       }
#     ]
#     crons = [
#       {
#         name  = "lambda-loves-cron"
#         arn   = aws_lambda_function.lambda_raw_data_to_ingestion_bucket.arn
#         input = jsonencode({"job": "cron-by-rate"})
#       }
#     ]
#   }
# }
