import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Retrieve job arguments passed to the Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize the Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job with the provided job name
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the predicate for pushdown filtering to reduce the dataset (filter by region)
predicate_pushdown = "region in ('ca','gb','us')"

# Read the raw YouTube statistics data from the Glue catalog (with pushdown filtering)
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_raw", 
    table_name="raw_statistics", 
    transformation_ctx="datasource0", 
    push_down_predicate=predicate_pushdown
)

# Apply mappings to select and rename fields
applymapping1 = ApplyMapping.apply(
    frame=datasource0, 
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "long"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "long"),
        ("likes", "long", "likes", "long"),
        ("dislikes", "long", "dislikes", "long"),
        ("comment_count", "long", "comment_count", "long"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string")
    ], 
    transformation_ctx="applymapping1"
)

# Resolve choice conflicts, if any (converts data types where necessary)
resolvechoice2 = ResolveChoice.apply(
    frame=applymapping1, 
    choice="make_struct", 
    transformation_ctx="resolvechoice2"
)

# Drop any null fields to clean the data
dropnullfields3 = DropNullFields.apply(
    frame=resolvechoice2, 
    transformation_ctx="dropnullfields3"
)

# Coalesce the DataFrame to a single partition for output
datasink1 = dropnullfields3.toDF().coalesce(1)

# Convert back to DynamicFrame for saving
df_final_output = DynamicFrame.fromDF(datasink1, glueContext, "df_final_output")

# Write the final output to S3 in Parquet format, partitioned by region
datasink4 = glueContext.write_dynamic_frame.from_options(
    frame=df_final_output, 
    connection_type="s3", 
    connection_options={
        "path": "s3://de-on-youtube-cleansed-useast1-dev/youtube/raw_statistics/",
        "partitionKeys": ["region"]
    }, 
    format="parquet", 
    transformation_ctx="datasink4"
)

# Commit the Glue job
job.commit()
