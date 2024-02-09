import boto3


def get_dynamodb():
  access_key = ""
  secret_key = ""
  region = ""
  return boto3.resource('dynamodb',
                 aws_access_key_id=access_key,
                 aws_secret_access_key=secret_key,
                 region_name=region)
  

def createTableIfNotExists(table_name):
    '''
    Create a DynamoDB table if it does not exist.
    This must be run on the Spark driver, and not inside foreach.
    '''
    dynamodb = get_dynamodb()

    existing_tables = dynamodb.meta.client.list_tables()['TableNames']
    if table_name not in existing_tables:
      print("Creating table %s" % table_name)
      table = dynamodb.create_table(
          TableName=table_name,
          KeySchema=[ { 'AttributeName': 'key', 'KeyType': 'HASH' } ],
          AttributeDefinitions=[ { 'AttributeName': 'key', 'AttributeType': 'S' } ],
          ProvisionedThroughput = { 'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5 }
      )

      print("Waiting for table to be ready")

    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

class SendToDynamoDB_ForeachWriter:
 

  def open(self, partition_id, epoch_id):
    
    self.dynamodb = get_dynamodb()
    return True

  def process(self, row):
    # This is called for each row after open() has been called.
    # This implementation sends one row at a time.
    # For further enhancements, contact the Spark+DynamoDB connector
    # team: https://github.com/audienceproject/spark-dynamodb
    self.dynamodb.Table("latest_movie").put_item(
        Item = {'adult': row['adult'],
            'backdrop_path': row['backdrop_path'],
            'genre_ids': row['genre_ids'],
            'id': row['id'],
            'media_type': row['media_type'],
            'original_language': row['original_language'],
            'original_title': row['original_title'],
            'overview':row['overview'],
            'popularity': row['popularity'],
            'poster_path': row['poster_path'],
            'release_date': row['release_date'],
            'title': row['title'],
            'video':row['video'],
            'vote_average':row['vote_average'],
            'vote_count': row['vote_count']})

  def close(self, err):
    # This is called after all the rows have been processed.
    if err:
      raise err