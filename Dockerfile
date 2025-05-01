# Stage 1: Builder - Install dependencies
# Use an AWS provided base image matching your target Lambda Runtime & Architecture.
# Using Python 3.11 on x86_64 as an example. Change if your Lambda uses arm64 or a different Python version.
FROM public.ecr.aws/lambda/python:3.11 as builder

# Set the working directory inside the builder stage
WORKDIR /build

# Install OS-level packages if needed by any Python dependencies
# Example: RUN yum update -y && yum install -y gcc some-dev-lib ...
# For boto3, google-genai, tavily - usually not needed, but keep in mind for complex libs.

# Copy the requirements file into the builder stage
COPY requirements.txt .

# Upgrade pip and install Python packages from requirements.txt into a target directory
# Using --platform manylinux2014_x86_64 might help ensure broader compatibility
# for compiled dependencies, although the base image often handles this.
RUN pip install --upgrade pip
RUN pip install -r requirements.txt --target ./lambda_packages
# Example with platform flag (usually not required with AWS base images):
# RUN pip install -r requirements.txt --target ./lambda_packages --platform manylinux2014_x86_64 --only-binary=:all:


# Stage 2: Final Lambda Image
# Use the same base image as the builder for consistency
FROM public.ecr.aws/lambda/python:3.11


# Set the working directory for the Lambda function code
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy installed packages from the builder stage's target directory
# This ensures only necessary installed libraries are included in the final image
COPY --from=builder /build/lambda_packages ./

# Copy your Lambda function source code AND the config_data directory
# This assumes your Docker build context is the project root directory
# ('trendforecast_ask_ai_service/') where 'src/' is located.
COPY src/ ./

# Set the CMD to your primary handler (e.g., for the API Gateway entry point Lambda)
# Note: This CMD is typically overridden by the Lambda function's Handler configuration in AWS,
# but it's good practice to set a default.
# Replace 'interpret_query_v2.lambda_handler' if your main entry point is different.
CMD ["interpret_query_v2.lambda_handler"]