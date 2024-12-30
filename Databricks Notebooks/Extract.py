%run "./reader_factory"


class Extract:
    def __init__(self):
        pass
    
    def extract(self):
        pass

class BAAPExtract(Extract):
    
    def __init__(self):
        self.dsfactobj = DataSourceFactory()
    
    def transform(self):

        transaction_df = self.dsfactobj.get_ds(
            src_type = "csv",
            path="dbfs:/FileStore/tables/Transaction_Updated.csv"
            ).get_df()

        customer_df = self.dsfactobj.get_ds(
            src_type = "delta",
            path = "default.customer_updated_delta"
        ).get_df()

        dataframe_dict = {
            "transaction_df": transaction_df,
            "customer_df": customer_df
        }

        return dataframe_dict

class BOIAExtract(Extract):
    
    def __init__(self):
        self.dsfactobj = DataSourceFactory()
    
    def transform(self):

        transaction_df = self.dsfactobj.get_ds(
            src_type = "csv",
            path="dbfs:/FileStore/tables/Transaction_Updated.csv"
            ).get_df()

        customer_df = self.dsfactobj.get_ds(
            src_type = "delta",
            path = "default.customer_updated_delta"
        ).get_df()

        dataframe_dict = {
            "transaction_df": transaction_df,
            "customer_df": customer_df
        }

        return dataframe_dict
    

class PPLExtract(Extract):
    
    def __init__(self):
        self.dsfactobj = DataSourceFactory()
    
    def transform(self):

        transaction_df = self.dsfactobj.get_ds(
            src_type = "csv",
            path="dbfs:/FileStore/tables/Transaction_Updated.csv"
            ).get_df()

        customer_df = self.dsfactobj.get_ds(
            src_type = "delta",
            path = "default.customer_updated_delta"
        ).get_df()

        dataframe_dict = {
            "transaction_df": transaction_df,
            "customer_df": customer_df
        }

        return dataframe_dict
