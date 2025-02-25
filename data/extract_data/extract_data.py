
class ExtractDataRepository:
    def __init__(self,database):
        self.database = database

    def get_all_from_table(self):
        string_get_query = "SELECT * FROM table;"
        get_result_list = self.database.run(string_get_query)
        columns = [col['name'] for col in self.database.columns]
        return [dict(zip(columns, result)) for result in get_result_list]

    def get_all_columns_names(self):
        # string_get_query = "SELECT * FROM treasures INNER JOIN shops USING (shop_id)"
        # get_result_list = self.database.run(string_get_query)
        # columns = [col["name"] for col in self.database.columns]
        # print(columns)
        # return columns

    def get_all_table_names(self):
        # string_get_query = "SELECT * FROM treasures INNER JOIN shops USING (shop_id)"
        # get_result_list = self.database.run(string_get_query)
        # columns = [col["name"] for col in self.database.columns]
        # print(columns)
        # return columns