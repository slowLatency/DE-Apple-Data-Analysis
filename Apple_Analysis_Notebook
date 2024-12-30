%run "./Extract"
%run "./Transform"
%run "./Load"

from pyspark.sql import SparkSession

class BAAPWorkflow:
    """Workflow for Customers who bought AirPods after iPhone"""
    
    def runner(self):

        input_dataframe_dict = BAAPExtract().transform() 

        BAAP_df = BAAPTransform().transform(input_dataframe_dict)
        
        BAAPLoad("DBFSPartition", BAAP_df, 'dbfs:/FileStore/BAAP', {"method":"overwrite", "partitionByCol":['location']}).load()

class BOIAWorkflow:
    """Workflow for Customers who bought only iPhones and AirPods"""

    def runner(self):

        input_dataframe_dict = BOIAExtract().transform() 

        BOIA_df = BOIATransform().transform(input_dataframe_dict)
        
        BOIALoad("DBFSPartition", BOIA_df, 'dbfs:/FileStore/BOIA', {"method":"overwrite", "partitionByCol":['location']}).load()

class PPLWorkflow:
    """Workflow for calculating Products ordred per Location"""

    def runner(self):

        input_dataframe_dict = PPLExtract().transform() 

        PPL_df = PPLTransform().transform(input_dataframe_dict)
        
        PPLLoad("DeltaLoad", PPL_df, 'default.products_per_loaction_fact', {"method":"overwrite", "partitionByCol":['location']}).load()


class Workflow:
    def __init__(self, wrkflw_nm):
        self.wrkflw_nm = wrkflw_nm
        self.wrkflw_dict = {
            "BAAPWorkflow": BAAPWorkflow,
            "BOIAWorkflow": BOIAWorkflow,
            "PPLWorkflow": PPLWorkflow
        }
    
    def runner(self):
        return self.wrkflw_dict[self.wrkflw_nm]()

wrkflw_nm = "PPLWorkflow"    
workflow = Workflow(wrkflw_nm)

workflow.runner().runner()
