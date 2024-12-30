from pyspark.sql.window import Window
from pyspark.sql.functions import col, lead, broadcast, collect_set, array_contains, size, when, count, lit

class Transform:
    def __init__(self):
        pass
    
    def transform(self):
        pass

class BAAPTransform(Transform):
    
    def transform(self, input_dataframe_dict):
        """
        Bought Airpods after iPhone
        
        """
        transaction_df = input_dataframe_dict.get("transaction_df")
        customer_df = input_dataframe_dict.get("customer_df")

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transaction_df = transaction_df.withColumn("next_product", lead("product_name").over(windowSpec))
        transaction_df = transaction_df.where((transaction_df.product_name == 'iPhone') & (transaction_df.next_product == 'AirPods'))
        final_df = broadcast(transaction_df).join((customer_df), on="customer_id", how="inner").select(customer_df.customer_id, customer_df.customer_name, customer_df.location)

        return final_df

class BOIATransform(Transform):
    
    def transform(self, input_dataframe_dict):
        """
        Bought only iPhone and Airpods
        
        """
        transaction_df = input_dataframe_dict.get("transaction_df")
        customer_df = input_dataframe_dict.get("customer_df")

        agg_transaction_df = transaction_df.groupBy("customer_id").agg(collect_set("product_name").alias("products"))
        new_agg_tranasaction_df = agg_transaction_df\
            .withColumn("boia", when((array_contains(col("products"), "iPhone") & (array_contains(col("products"), "AirPods") & (size(col("products")) == 2) )), "True").otherwise("False"))
        filtered_agg_transaction_df = new_agg_tranasaction_df.where(col("boia") == "True")
        final_df = broadcast(filtered_agg_transaction_df)\
            .join(customer_df, on="customer_id", how="inner").select(customer_df.customer_id, customer_df.customer_name, customer_df.location)
        
        return final_df
    
class PPLTransform(Transform):
    
    def transform(self, input_dataframe_dict):
        """
        Products per Location
        
        """
        transaction_df = input_dataframe_dict.get("transaction_df")
        customer_df = input_dataframe_dict.get("customer_df")

        trans_join_cus_df = transaction_df.join(customer_df, "customer_id")
        agg_trans_join_cus_df = trans_join_cus_df.groupBy("location").agg(count(lit(1)).alias("m_prod_per_loc"))
        final_df = agg_trans_join_cus_df.orderBy(col("m_prod_per_loc").desc(), col("location").asc())
        
        return final_df
        
