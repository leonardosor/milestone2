#!/bin/bash

# Census Data ETL - Deployment Script
# This script helps set up the AWS infrastructure and configure the ETL pipeline

set -e

echo "=== Census Data ETL Deployment Script ==="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if AWS CLI is installed
check_aws_cli() {
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed. Please install it first."
        echo "Visit: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
        exit 1
    fi
    print_status "AWS CLI is installed"
}

# Check if Python is installed
check_python() {
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is not installed. Please install it first."
        exit 1
    fi
    print_status "Python 3 is installed"
}

# Install Python dependencies
install_dependencies() {
    print_status "Installing Python dependencies..."
    pip3 install -r requirements.txt
    print_status "Dependencies installed successfully"
}

# Deploy CloudFormation stack
deploy_infrastructure() {
    print_status "Deploying AWS infrastructure..."
    
    # Check if stack name is provided
    if [ -z "$1" ]; then
        STACK_NAME="census-data-etl"
    else
        STACK_NAME="$1"
    fi
    
    # Get VPC and subnet information
    echo ""
    print_warning "You need to provide VPC and subnet information for the RDS instance."
    echo "You can find this information in the AWS Console under VPC > Your VPCs"
    echo ""
    
    read -p "Enter your VPC ID: " VPC_ID
    read -p "Enter subnet IDs (comma-separated, e.g., subnet-123,subnet-456): " SUBNET_IDS
    
    # Convert comma-separated subnets to CloudFormation format
    SUBNET_LIST=$(echo $SUBNET_IDS | sed 's/,/","/g' | sed 's/^/["/' | sed 's/$/"]/')
    
    # Deploy the CloudFormation stack
    aws cloudformation deploy \
        --template-file aws-rds-template.yaml \
        --stack-name $STACK_NAME \
        --parameter-overrides \
            VpcId=$VPC_ID \
            SubnetIds=$SUBNET_LIST \
        --capabilities CAPABILITY_IAM
    
    print_status "Infrastructure deployed successfully"
    
    # Get the outputs
    DB_ENDPOINT=$(aws cloudformation describe-stacks \
        --stack-name $STACK_NAME \
        --query 'Stacks[0].Outputs[?OutputKey==`DBEndpoint`].OutputValue' \
        --output text)
    
    DB_PORT=$(aws cloudformation describe-stacks \
        --stack-name $STACK_NAME \
        --query 'Stacks[0].Outputs[?OutputKey==`DBPort`].OutputValue' \
        --output text)
    
    print_status "Database endpoint: $DB_ENDPOINT"
    print_status "Database port: $DB_PORT"
}

# Configure the application
configure_app() {
    print_status "Configuring the application..."
    
    # Get database credentials
    echo ""
    print_warning "Please provide database credentials:"
    read -p "Database username: " DB_USERNAME
    read -s -p "Database password: " DB_PASSWORD
    echo ""
    read -p "Database name (default: census_data): " DB_NAME
    DB_NAME=${DB_NAME:-census_data}
    
    # Get AWS region
    read -p "AWS region (default: us-east-1): " AWS_REGION
    AWS_REGION=${AWS_REGION:-us-east-1}
    
    # Update config.json
    cat > config.json << EOF
{
    "database": {
        "host": "$DB_ENDPOINT",
        "port": $DB_PORT,
        "database": "$DB_NAME",
        "username": "$DB_USERNAME",
        "password": "$DB_PASSWORD"
    },
    "aws": {
        "region": "$AWS_REGION",
        "secret_name": "census-database-credentials"
    },
    "use_aws_secrets": false,
    "census": {
        "api_key": "your_census_api_key_here",
        "rate_limit_delay": 1
    },
    "etl": {
        "batch_size": 1000,
        "begin_year": 2015,
        "end_year": 2019
    }
}
EOF
    
    print_status "Configuration file created: config.json"
    print_warning "Please update the Census API key in config.json before running the ETL process"
}

# Test database connection
test_connection() {
    print_status "Testing database connection..."
    
    # Create a simple test script
    cat > test_connection.py << 'EOF'
import json
import psycopg2
from sqlalchemy import create_engine, text

try:
    # Load config
    with open('config.json', 'r') as f:
        config = json.load(f)
    
    db_creds = config['database']
    
    # Test direct connection
    conn = psycopg2.connect(
        host=db_creds['host'],
        port=db_creds['port'],
        database=db_creds['database'],
        user=db_creds['username'],
        password=db_creds['password']
    )
    
    print("✓ Direct database connection successful")
    conn.close()
    
    # Test SQLAlchemy connection
    connection_string = f"postgresql://{db_creds['username']}:{db_creds['password']}@{db_creds['host']}:{db_creds['port']}/{db_creds['database']}"
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("✓ SQLAlchemy connection successful")
    
    print("✓ All connection tests passed!")
    
except Exception as e:
    print(f"✗ Connection test failed: {e}")
    exit(1)
EOF
    
    python3 test_connection.py
    rm test_connection.py
}

# Main deployment function
main() {
    echo "Starting deployment process..."
    echo ""
    
    # Check prerequisites
    check_aws_cli
    check_python
    install_dependencies
    
    echo ""
    print_status "Prerequisites check completed"
    echo ""
    
    # Ask user what they want to do
    echo "What would you like to do?"
    echo "1. Deploy AWS infrastructure (RDS PostgreSQL)"
    echo "2. Configure application (update config.json)"
    echo "3. Test database connection"
    echo "4. Run complete deployment"
    echo "5. Exit"
    echo ""
    
    read -p "Enter your choice (1-5): " choice
    
    case $choice in
        1)
            deploy_infrastructure $2
            ;;
        2)
            configure_app
            ;;
        3)
            test_connection
            ;;
        4)
            deploy_infrastructure $2
            configure_app
            test_connection
            print_status "Deployment completed successfully!"
            print_warning "Don't forget to update the Census API key in config.json"
            ;;
        5)
            print_status "Exiting..."
            exit 0
            ;;
        *)
            print_error "Invalid choice. Please enter a number between 1-5."
            exit 1
            ;;
    esac
}

# Run main function
main "$@" 