# joe to do

resource "aws_sfn_state_machine" "lambda_1_2_3" {
  name     = "lambda-2-state_machine"
  role_arn = aws_iam_role.step_function_execution_role.arn

  definition = templatefile("${path.module}/pipeline.json",
    { resource          = aws_lambda_function.lambda_ingestion_to_processed_bucket.arn
      aws_region        = data.aws_region.current.name,
      aws_account_num   = data.aws_caller_identity.current.account_id,
      function_name     = var.step_function_name
    })
}
