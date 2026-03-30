from phenopipe.tasks.get_data.get_data import GetData


class FixedQuery(GetData):
    query: str

    def _complete(self):
        """
        Query a fixed sql query from fixed queries vocabulary and update self.output with resulting dataframe
        """
        self.output = self.env_vars["query_conn"].get_query(
            self.query, self.lazy
        )

    def set_output_dtypes_and_names(self):
        self.set_date_column_dtype(self.date_col)
