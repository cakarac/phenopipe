import polars as pl
from phenopipe.tasks.task import Task


class CombinePts(Task):
    def _complete(self):
        """
        Stack multiple phenotype task outputs into one table.
        Inputs:
        -------
         - any number of phenotype tasks in input_tasks
         - each task output must include its task-level person_col and date_col

        Output:
        -------
         - stacked phenotype dataframe
         - one earliest date per person_id
        """
        if len(self.input_tasks) == 0:
            raise ValueError("At least one phenotype input task is required")

        missing_in_input_tasks = [k for k in self.inputs if k not in self.input_tasks]
        if missing_in_input_tasks:
            raise ValueError(
                f"This task is designed to run with input_tasks in order to have access to needed column names. Missing in input_tasks: {missing_in_input_tasks}"
            )

        frames = []
        for phenotype_name, task in self.input_tasks.items():
            df = task.output
            if df is None:
                raise ValueError(
                    f"Input task '{phenotype_name}' has no output. Ensure it is completed first."
                )

            source_person_col = task.person_col
            source_date_col = task.date_col

            if df.collect_schema().get(source_date_col) == pl.String:
                df = df.with_columns(pl.col(source_date_col).str.to_date())

            # Standardize upstream task identifiers to this task's person/date columns.
            df = df.rename(
                {
                    source_person_col: self.person_col,
                    source_date_col: self.date_col,
                }
            )

            schema = df.collect_schema()

            if schema.get(self.person_col) != pl.String:
                df = df.with_columns(pl.col(self.person_col).cast(pl.String))

            if schema.get(self.date_col) != pl.Date:
                df = df.with_columns(pl.col(self.date_col).cast(pl.Date))

            df = df.select([self.person_col, self.date_col])

            frames.append(df)

        self.output = pl.concat(frames)
        self.output = self.output.group_by(self.person_col).agg(
            pl.col(self.date_col).min().alias(self.date_col)
        )
