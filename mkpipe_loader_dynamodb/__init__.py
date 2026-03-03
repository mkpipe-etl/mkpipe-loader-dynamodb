import gc
import json
from datetime import datetime
from decimal import Decimal

from mkpipe.spark.base import BaseLoader
from mkpipe.spark.columns import add_etl_columns
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.utils import get_logger

logger = get_logger(__name__)


class _DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if o == int(o):
                return int(o)
            return float(o)
        return super().default(o)


def _convert_value(value):
    """Convert Python values to DynamoDB-compatible types."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: _convert_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_convert_value(v) for v in value]
    return str(value)


class DynamoDBLoader(BaseLoader, variant='dynamodb'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.region = connection.region or 'us-east-1'
        self.aws_access_key = connection.aws_access_key
        self.aws_secret_key = connection.aws_secret_key

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        write_mode = data.write_mode
        df = data.df

        if df is None:
            logger.info({'table': target_name, 'status': 'skipped', 'reason': 'no data'})
            return

        df = add_etl_columns(df, datetime.now(), dedup_columns=table.dedup_columns)

        logger.info({
            'table': target_name,
            'status': 'loading',
            'write_mode': write_mode,
        })

        import boto3

        session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.region,
        )
        dynamodb = session.resource('dynamodb')
        ddb_table = dynamodb.Table(target_name)

        if write_mode == 'overwrite':
            scan_kwargs = {}
            key_names = [k['AttributeName'] for k in ddb_table.key_schema]
            while True:
                response = ddb_table.scan(**scan_kwargs, ProjectionExpression=', '.join(key_names))
                with ddb_table.batch_writer() as batch:
                    for item in response.get('Items', []):
                        batch.delete_item(Key=item)
                if 'LastEvaluatedKey' not in response:
                    break
                scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']

        rows = [row.asDict(recursive=True) for row in df.collect()]
        batchsize = table.batchsize or 10000

        with ddb_table.batch_writer() as batch:
            for i, row in enumerate(rows):
                item = {k: _convert_value(v) for k, v in row.items() if v is not None}
                batch.put_item(Item=item)

        df.unpersist()
        gc.collect()

        logger.info({
            'table': target_name,
            'status': 'loaded',
            'rows': len(rows),
        })
