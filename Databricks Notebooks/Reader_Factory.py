from pyspark.sql.functions import to_timestamp

class DataSourceFactory:
    """ Factory Class """
    
    def get_ds(self, src_type, path):
        if src_type == "csv":
            return CsvReader(path)
        elif src_type == "parquet":
            return PrqReader(path)
        elif src_type == "delta":
            return DeltaReader(path)
        else:
            raise ValueError(f"{src_type} is an invalid type")

class CsvReader:

    def __init__(self,path):
        self.path = path
    
    def get_df(self):
        return spark.read.format("csv").option("header", "true").load(self.path)

class PrqReader:

    def __init__(self,path):
        self.path = path
    
    def get_df(self):
        return spark.read.format("parquet").load(self.path)
    
class DeltaReader:

    def __init__(self,path):
        self.path = path
    
    def get_df(self):
        return spark.table(self.path).withColumn("join_date", to_timestamp(col('join_date'), 'yyyy-MM-dd HH:mm:ss'))
