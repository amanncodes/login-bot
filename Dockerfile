# AWS Lambda Container Image for Instagram Comments Scraper
# Uses official AWS Lambda Python base image with OpenTelemetry support

FROM public.ecr.aws/lambda/python:3.11

# Set working directory
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy requirements file
COPY lambda_requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r lambda_requirements.txt

# Copy Lambda function code
COPY instagram_comments_lambda.py .
COPY lambda_telemetry.py .

# Set the CMD to your handler
CMD ["instagram_comments_lambda.lambda_handler"]
