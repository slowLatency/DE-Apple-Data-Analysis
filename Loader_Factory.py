class DataLoadFactory:
    def __init__(self):
        pass

    def get_des(self, des_type, res_df, path, params):
        if des_type == "DBFS":
            return DBFSLoad(res_df, path, params)
        elif des_type == "DBFSPartition":
            return DBFSLoadWithPartition(res_df, path, params)
        elif des_type == "DeltaLoad":
            return DeltaLoad(res_df, path, params)
        else:
            print(f"load hasn't been implemented for {des_type}")
            # raise.ValueException(f"load hasn't been implemented for {des_type}")

class DBFSLoad:
    def __init__(self, res_df, path, params):
        self.res_df = res_df
        self.path = path
        self.params = params

    def load_df(self):
        self.res_df.write.mode(self.params.get("method")).save(self.path)

class DBFSLoadWithPartition:
    def __init__(self, res_df, path, params):
        self.res_df = res_df
        self.path = path
        self.params = params

    def load_df(self):
        self.res_df.write.mode(self.params.get("method")).partitionBy(*self.params.get("partitionByCol")).save(self.path)

class DeltaLoad:
    def __init__(self, res_df, path, params):
        self.res_df = res_df
        self.path = path
        self.params = params

    def load_df(self):
        self.res_df.write.mode(self.params.get("method")).partitionBy(*self.params.get("partitionByCol")).saveAsTable(self.path)
