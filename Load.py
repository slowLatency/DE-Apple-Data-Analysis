%run "./loader_factory"

class Load:
    def __init__(self):
        pass

    def load(self):
        pass

class BAAPLoad(Load):
    def __init__(self, des_type, res_df, path, params):
        self.dlfactobj = DataLoadFactory()
        self.des_type = des_type
        self.res_df = res_df
        self.path = path
        self.params = params

    def load(self):
        self.dlfactobj.get_des(self.des_type, self.res_df, self.path, self.params).load_df()
    
class BOIALoad(Load):
    def __init__(self, des_type, res_df, path, params):
        self.dlfactobj = DataLoadFactory()
        self.des_type = des_type
        self.res_df = res_df
        self.path = path
        self.params = params

    def load(self):
        self.dlfactobj.get_des(self.des_type, self.res_df, self.path, self.params).load_df()

class PPLLoad(Load):
    def __init__(self, des_type, res_df, path, params):
        self.dlfactobj = DataLoadFactory()
        self.des_type = des_type
        self.res_df = res_df
        self.path = path
        self.params = params

    def load(self):
        self.dlfactobj.get_des(self.des_type, self.res_df, self.path, self.params).load_df()
