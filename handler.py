import json
import duckdb
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def lambda_handler(event=None, context=None):
    try:
        logger.info("Initializing DuckDB connection")
        conn = duckdb.connect(':memory:')

        # Install and load required extensions
        extensions = ['aws', 'httpfs', 'iceberg', 'parquet']
        for ext in extensions:
            conn.execute(f"INSTALL {ext};")
            conn.execute(f"LOAD {ext};")
            logger.info(f"Installed and loaded {ext} extension")

        # Force install iceberg from core_nightly
        conn.execute("FORCE INSTALL iceberg FROM core_nightly;")
        conn.execute("LOAD iceberg;")
        logger.info("Forced installation and loaded iceberg from core_nightly")

        # Set up AWS credentials
        conn.execute("CALL load_aws_credentials();")
        conn.execute("""
        CREATE SECRET (
            TYPE s3,
            PROVIDER credential_chain
        );
        """)
        logger.info("Set up AWS credentials")

        # Validate input parameters
        required_params = ['query', 'catalog_arn']
        if not all(event.get(param) for param in required_params):
            return {
                'statusCode': 400,
                'body': json.dumps(f'Missing required parameters: {required_params}')
            }

        catalog_arn = event['catalog_arn']
        query = event['query']

        # Attach S3 Tables catalog using ARN
        try:
            conn.execute(f"""
            ATTACH '{catalog_arn}' 
            AS s3_tables_db (
                TYPE iceberg,
                ENDPOINT_TYPE s3_tables
            );
            """)
            logger.info(f"Successfully attached S3 Tables catalog: {catalog_arn}")
        except Exception as e:
            logger.error(f"Catalog attachment failed: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Catalog connection failed',
                    'details': str(e)
                })
            }

        # Execute query with enhanced error handling
        try:
            result = conn.execute(query).fetchall()
            columns = [desc[0] for desc in conn.description]

            formatted = [dict(zip(columns, row)) for row in result]

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'data': formatted,
                    'metadata': {
                        'row_count': len(formatted),
                        'column_names': columns
                    }
                }, default=str)
            }

        except Exception as query_error:
            logger.error(f"Query execution failed: {str(query_error)}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Query execution error',
                    'details': str(query_error),
                    'suggestions': [
                        "Verify table exists in the attached database",
                        "Check your S3 Tables ARN format",
                        "Validate AWS permissions for the bucket"
                    ]
                })
            }

    except Exception as e:
        logger.error(f"Global error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Unexpected runtime error'})
        }
    finally:
        if 'conn' in locals():
            conn.close()


# Local test configuration
if __name__ == "__main__":
    os.environ['AWS_ACCESS_KEY_ID'] = os.getenv("AWS_ACCESS_KEY_ID")
    os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv("AWS_SECRET_ACCESS_KEY")
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    import duckdb

    print(duckdb.__version__)

    test_event = {
        "query": "SELECT * FROM s3_tables_db.s3tablescatalog.daily_sales LIMIT 10;",
        "catalog_arn": "arn:aws:s3tables:us-east-1:<ACCOUNT>:bucket/<BUCKETNAME>"
    }

    print(lambda_handler(test_event))
