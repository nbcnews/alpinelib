import argparse
import boto3
import difflib
from alpinelib import logging

logger = logging.getFormattedLogger()

def fetch_table_metadata(catalog_name, database_name):
    logger.info('Fetching table metadata')
    client = boto3.client('athena')
    data = client.list_table_metadata(
        CatalogName=catalog_name,
        DatabaseName=database_name,
        MaxResults=50
    )
    tables = data.get('TableMetadataList')
    next_token = data.get('NextToken')
    while next_token:
        data = client.list_table_metadata(
            CatalogName=catalog_name,
            DatabaseName=database_name,
            NextToken=next_token,
            MaxResults=50
        )
        tables = tables + data.get('TableMetadataList')
        next_token = data.get('NextToken')
    logger.info(f'Fetched metadata for {len(tables)} tables')
    return tables

def schema_comparison(catalog_name, database_name, file):
    tables = fetch_table_metadata(catalog_name, database_name)
    schema_dict = {}
    for table in tables:
        name = table['Name']
        columns = '\n'.join(str(i) for i in table['Columns'])
        if columns in schema_dict:
            schema_dict[columns].append(name)
        else:
            schema_dict[columns] = [name]
    logger.info(f'There are {len(schema_dict)} total schemas')
    schemas = list(schema_dict.keys())
    for i in range(len(schemas)):
        logger.info(f'Schema {i+1} shared by tables: {schema_dict[schemas[i]]}')
    with open(file, 'w') as f:
        for i in range(len(schemas)):
            for j in range(i+1, len(schemas)):
                f.write(f'Schema {i+1} vs schema {j+1}\n')
                diff = difflib.ndiff(
                    schemas[i].splitlines(keepends=True),
                    schemas[j].splitlines(keepends=True)
                )
                f.write(''.join(diff))
                f.write('\n\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('catalog', type=str)
    parser.add_argument('database', type=str)
    parser.add_argument('--file', type=str, default='schema_comparison.txt')
    args = parser.parse_args()
    schema_comparison(args.catalog, args.database, args.file)

